# Shared aggregation: correctness banked, performance pending

**All 100M timings collected before the rig repair are provisional.** They are not delivery evidence, and this change has no accepted performance gain or ranking claim. Firstmate's 8 September rig-handover instruction supersedes the earlier measurement acceptance: nominally comparable runs varied too much to establish the effect of these changes. Performance acceptance and delivery await the repaired rig and a new window granted by Firstmate.

## Implementation

- `NumericGroupAggTable` supports sequential accumulator records behind a compact hash index. Full keys and exact identity lanes still prove equality; record handles survive index growth. `GroupTableSpill` owns the scan-local index recycler and enables the layout only for **stride >= 5**. At the three-quarter growth threshold, sparse storage reserves `(4/3) * stride` lanes per live group, versus `stride + 4/3` for dense records plus the index. They cross at stride 4. The gate records this rationale, and `sirix.projection.groupTable.denseIndex=false` retains the sparse layout.
- Integral COUNT/SUM/AVG composite groups ordered solely by row count use present-count/sum operand pairs. Only winning accumulators expand into the ordinary output layout. Other orders, HAVING, string/deferred operands, and MIN/MAX retain ordinary state.
- Sliced numeric/composite DISTINCT aggregation feeds worker batches directly. Singleton sets store their first value inline; promotion retains the previous 16-entry set sizing. Disjoint group stripes publish final counts on the existing worker pool, and merge bucketing visits the shards without building one intermediate map.
- Incompatible-table diagnostics now include the `sumsOnly` flags, including the case where all other layout properties agree.

No additional input scan, global dictionary or data cache, persisted-format change, or database rebuild is introduced. Dictionary value comparisons, row multiplicity, checked sum overflow, missing operands, stable ties, existing pass budgets, and four distinct value stripes remain unchanged. The benchmark runner is unmodified and uninstrumented.

## Correctness and recovery

The narrowed implementation passed **137 focused tests** and the full **1M** DuckDB gate: **0 mismatch, 0 missing, 0 unverifiable, and 0 serving declines**. All 43 outputs were byte-identical to the original 1M baseline. The database has two segments. Both completed post-narrowing 100M pairs also produced 43 byte-identical outputs without serving declines; their timing data remains provisional.

The no-mistakes review-fix round reached its 30-minute timeout before committing. Its managed worktree was removed and branch custody returned to the worker. All six edited source/test files were recovered from the tool transcript and matched the source hashes recorded before validation byte-for-byte. Subsequent local changes were formatting and two comment corrections; production tokens remain unchanged. A fresh local run of the three affected test classes passed **15 tests** with no failures, errors or skips. The original tested files, frozen runtime, correctness logs, result files, source hashes and completed run manifests are preserved under `bundles/sirix-query/build/diagnostics/groupby/recovered-fix-source/`, `nm-exclusive2-evidence/` and `nm-exclusive3-evidence/`.

The [provisional evidence JSON](CLICKBENCH_GROUP_AGGREGATION_2026-09-08.json) retains raw per-try wall/CPU times and output hashes for those two completed narrowed-code pairs. It explicitly disallows using those timings for acceptance. It contains no selected best pair or accepted score. Previous timings remain historical diagnostics, superseded for delivery purposes.

## Rig ownership and measurement requirements

The last completed pair released the rig lock at **2026-09-08T03:39:26.704902+00:00**. The rig-repair lane owns 100M access until Firstmate grants another window. Do not run another 100M leg, tune against these provisional timings, or ship a performance claim from them.

Exclusive access remains necessary: hold one shared rig lock continuously across baseline and candidate, inspect live database JVMs after acquisition and between legs, and inherit the lock in child JVMs. A lock file's existence does not prove a live holder; never unlink a potentially held lock. Preserve the **6/14 GiB heap and 10 GiB arena** envelope, freeze the artifacts before measurement, and record source/artifact identity, complete flags and lock times. The rig repair must additionally establish repeatability; exclusivity alone did not make the collected timings dependable.

Compare the score across all 43 queries as well as elapsed seconds after the rig is validated. A decrease in total suite time does not establish an improvement in the geometric-mean score. Rig mechanics remain in [the segment-lane handoff](HANDOFF_SEGMENT_LANE_2026-09-06.md) and `bundles/sirix-query/bench/clickbench/rig/`.
