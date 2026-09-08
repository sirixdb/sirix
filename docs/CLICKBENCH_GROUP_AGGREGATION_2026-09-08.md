# Shared aggregation: correctness validation, performance unverified

**The performance effect is unverified pending measurement resolution.** All earlier 100M timings are provisional; this change makes no accepted speedup or ranking claim. Correctness review, tests and CI may proceed to a green PR now. Firstmate will hold the merge until reliable measurement establishes whether to land or withdraw the change.

Firstmate's completed variance study reports a **minimum detectable effect of approximately 1.345 ln** for this harness on the current machine. A single paired run therefore cannot reliably distinguish a small gain from measurement noise. This is a measurement-resolution limit, not a performance result for this change. The study largely eliminated GC, CPU placement and thermal effects as explanations for the remaining noise; continuing JIT compilation during timed tries remains a hypothesis, with the cause unresolved. These findings supersede the earlier thermal diagnosis and acceptance of provisional timings.

## Implementation

- `NumericGroupAggTable` supports sequential accumulator records behind a compact hash index. Full keys and exact identity lanes still prove equality; record handles survive index growth. `GroupTableSpill` owns the scan-local index recycler and enables the layout only for **stride >= 5**. At the three-quarter growth threshold, sparse storage reserves `(4/3) * stride` lanes per live group, versus `stride + 4/3` for dense records plus the index. They cross at stride 4. The gate records this rationale, and `sirix.projection.groupTable.denseIndex=false` retains the sparse layout.
- Integral COUNT/SUM/AVG composite groups ordered solely by row count use present-count/sum operand pairs. Only winning accumulators expand into the ordinary output layout. Other orders, HAVING, string/deferred operands, and MIN/MAX retain ordinary state.
- Sliced numeric/composite DISTINCT aggregation feeds worker batches directly. Singleton sets store their first value inline; promotion retains the previous 16-entry set sizing. Disjoint group stripes publish final counts on the existing worker pool, and merge bucketing visits the shards without building one intermediate map.
- Incompatible-table diagnostics now include the `sumsOnly` flags, including the case where all other layout properties agree.
- Aborted-pass cleanup releases the finished workers' tables first and drains the scan-local probe-index recycler last, so every chunk is back in a pool before the restart measures live heap. The existing shared payload-pool guard remains in place.

No additional input scan, global dictionary or data cache, persisted-format change, or database rebuild is introduced. Dictionary value comparisons, row multiplicity, checked sum overflow, missing operands, stable ties, existing pass budgets, and four distinct value stripes remain unchanged. The benchmark runner is unmodified and uninstrumented.

## Correctness and recovery

The narrowed implementation passed **137 focused tests** and the full **1M** DuckDB gate: **0 mismatch, 0 missing, 0 unverifiable, and 0 serving declines**. All 43 outputs were byte-identical to the original 1M baseline. The database has two segments. Both completed post-narrowing 100M pairs also produced 43 byte-identical outputs without serving declines; their timing data remains provisional.

The no-mistakes review-fix round reached its 30-minute timeout before committing. Its managed worktree was removed and branch custody returned to the worker. All six edited source/test files were recovered from the tool transcript and matched the source hashes recorded before validation byte-for-byte. Subsequent local changes were formatting and two comment corrections; production tokens remain unchanged. A fresh local run of the three affected test classes passed **15 tests** with no failures, errors or skips. The original tested files, frozen runtime, correctness logs, result files, source hashes and completed run manifests are preserved under `bundles/sirix-query/build/diagnostics/groupby/recovered-fix-source/`, `nm-exclusive2-evidence/` and `nm-exclusive3-evidence/`.

The [provisional evidence JSON](CLICKBENCH_GROUP_AGGREGATION_2026-09-08.json) retains raw per-try wall/CPU times and output hashes for those two completed narrowed-code pairs. It explicitly disallows using those timings for acceptance. It contains no selected best pair or accepted score. Previous timings remain historical diagnostics, superseded for delivery purposes.

The subsequent probe-pool cleanup fix passed **88 tests** across 11 core test classes, with no failures, errors or skips. Its regression test exercises pooling, reuse and aborted-pass release; removing the drain reproduced the failure. The committed fix and test were recovered together after a paused review timed out. A later review also timed out without findings; neither timeout invalidated or replaced the preserved test evidence. Fresh pipeline validation is required for delivery.

## Rig ownership and measurement requirements

The instrument lane has released the box for builds, tests and review after completing its variance study. **No 100M work is authorized for this lane.** This validation run must not launch a 100M leg, profile or warm up the 100M database, tune against provisional timings, or publish a performance claim. Focused tests and 1M correctness validation remain authorized. Firstmate retains the merge hold and must explicitly grant any future measurement window.

For a future authorized window, exclusive access remains necessary: hold one shared rig lock continuously across baseline and candidate, inspect live database JVMs after acquisition and between legs, and inherit the lock in child JVMs. A lock file's existence does not prove a live holder; never unlink a potentially held lock. Preserve the **6/14 GiB heap and 10 GiB arena** envelope, freeze the artifacts before measurement, and record source/artifact identity, complete flags and lock times. Measurement must resolve the proposed effect against the observed noise floor; exclusivity alone did not make the collected timings dependable.

Compare the score across all 43 queries as well as elapsed seconds after the rig is validated. A decrease in total suite time does not establish an improvement in the geometric-mean score. Rig mechanics remain in [the segment-lane handoff](HANDOFF_SEGMENT_LANE_2026-09-06.md) and `bundles/sirix-query/bench/clickbench/rig/`.
