# Why seven passes: existing 100M evidence and memory arithmetic

Baseline b815d459d1218ab9081f360257ae6f16eff9f476. This is step 1 of Firstmate inbox 002, before any pass-count implementation or paired comparison. No additional benchmark run was used. Source: full-suite profile `build/hicard/profile-full-02/diagnostic/suite.log`; the relevant lines are preserved in `pass-budget-diagnostics.log`. Units labelled MB by the engine are MiB.

## What the engine computed

| Query | Abort-time estimatedTotalGroups | Initial groupBudget | Budget at restart | bytesPerGroup | Stride lanes | recommendedPasses / actual |
|---|---:|---:|---:|---:|---:|---:|
| q16 | 24,555,637 | 13,451,922 | 13,451,922 | 112 | 7 | 2 / 2 |
| q18 | 54,376,234 | 11,874,436 | 14,680,064 after GC | 128 | 8 | 4 / 4 |
| q32 | 100,007,737 | 14,680,064 | 14,680,064 | 128 | 11 | 7 / 7 |

All use 1,024 partitions. The largest range is ceil(1024/passes) partitions; `passesFor` chooses the smallest count whose expected largest range fits. For q32, six passes need ceil(100,007,737 * 171/1024) = 16,700,511 groups; seven need ceil(100,007,737 * 147/1024) = 14,356,580. Only seven fits 14,680,064. q18 needs 18,160,813 groups at three passes and 13,594,059 at four. q16 needs 24,555,637 at one and 12,277,819 at two. These are estimates from the aborted execution, not exact output cardinalities; subsequent executions memo completed cardinalities and replay the completed pass counts.

`groupBudget = min(maxHeap/8, (headroom + retainedPoolBytes)/4) / bytesPerGroup`, floored at 2^20 and capped at 2^26 groups. `bytesPerGroup = min(128, 16 * stride)`. At 14 GiB maxHeap, maxHeap/8 is 1,792 MiB: **1.75 GiB is a hard planning cap even on a completely empty heap**. This is why another GC cannot lower q32's pass count.

## Where memory lives and how much was free

NumericGroupAggTable payload/index chunks and GroupTableSpill.StripeBuffer chunks are `long[]` on the Java heap. LongChunkPool recycles heap arrays. None uses the off-heap allocator. The 10 GiB FrameSlotAllocator budget backs page caches and storage reads; it is unavailable to these tables, but it is not evidence of 10 GiB unused memory. Existing diagnostics did not record its per-query free capacity. Moving aggregation into it would require explicit allocation/ownership and arbitration with page-cache frames, not merely adding ten to the heap budget.

| Query / hot try | GC live MiB | Instant heap used MiB | Instant free to Xmx MiB | GC headroom MiB | Reusable pool MiB | Actual group budget |
|---|---:|---:|---:|---:|---:|---:|
| q16 / 2 | 9,360 | 10,073 | 4,263 | 4,976 | 865 | 13,673,435 |
| q16 / 3 | 8,443 | 9,355 | 4,981 | 5,893 | 865 | 15,818,451 |
| q18 / 2 | 8,013 | 8,910 | 5,426 | 6,323 | 922 | 14,680,064 |
| q18 / 3 | 8,003 | 8,610 | 5,726 | 6,333 | 922 | 14,680,064 |
| q32 / 2 | 8,155 | 8,273 | 6,063 | 6,181 | 1,426 | 14,680,064 |
| q32 / 3 | 8,155 | 9,091 | 5,245 | 6,181 | 1,426 | 14,680,064 |

HeapHeadroom uses the smaller of two collector records, each bounded by current pool usage. This is a planning estimate, not a freshly measured live set. At q32 the aggregation can reuse 6,181+1,426 = 7,607 MiB on that accounting, yet is allowed only 1,792 MiB, leaving 5,815 MiB outside its allowance. The printed shareMB=1545 excludes the reusable pool, whereas groupBudget adds it before the quarter-share calculation. This explains the apparent discrepancy. Instant free space is smaller than GC headroom because it includes uncollected allocations; neither should be described as guaranteed free memory.

## Counterfactual: yes to two passes within the existing heap

A dense record already stores only `stride * 8` payload bytes, plus a compact index. Index power-of-two/load-factor slack is below 8/3 eight-byte entries per group. Charge three whole index lanes (24 B/group), rather than doubling the entire record. This gives q16 80, q18 88 and q32 112 B/group. Chunk tails, temporary copies, worker state and collection slack still require a reserve.

A **grouped-aggregate-only** allowance of `min(maxHeap/2, 3*(GC headroom + reusable pool)/4)` leaves one quarter of effective headroom outside the allowance; the existing 2^26-group cap remains. It must apply only to bounded aggregates without distinct state. Do not raise HeapHeadroom.plannedShareBytes globally: it also sizes retained column fills and distinct sets, which would compete for the same increase.

Using the more constrained hot-try figures:

| Query | Effective headroom MiB | Proposed share MiB | Dense charge B/group | Proposed group budget (cap included) | Predicted passes |
|---|---:|---:|---:|---:|---:|
| q16 | 5,841 | 4,380.75 | 80 | 57,419,366 | 1 |
| q18 | 7,245 | 5,433.75 | 88 | 64,746,589 | 1 |
| q32 | 7,607 | 5,705.25 | 112 | 53,414,180 | 2 |

q32 two-pass largest share is about 50 million groups: 50,003,869 * 112 = 5.216 GiB, below the 5.571 GiB allowance. Its actual dense payload alone is 4.098 GiB; all 512 full compact indexes at 262,144 buckets each would add 1 GiB. In this executor final partition tables are selected and released incrementally, so they need not all coexist with their input buffers. A 25% reserve leaves 1.857 GiB for overlap/workers/GC, beyond the conservative index charge. At 20 workers, the default flush threshold plus one 64-leaf morsel bounds roughly 2.62 million worker records for 1,024-row leaves, about 280 MiB at 112 B each. Twenty simultaneous partition merges duplicate roughly 168 MiB of payload. There is room for chunk tails and temporary indexes, but this is a model to validate, not a measured candidate peak.

One-pass q32 at the same conservative charge requires 10.431 GiB, exceeding the 5.571 GiB allowance. **Two passes are feasible by this arithmetic without touching the arena or changing the pinned envelope; one pass is not justified by the recorded heap headroom.** The proposed smaller queries fit one pass even using the larger completed q18 estimate recorded by the selected-query profile (56.4 million groups). Cold estimates can be wrong; retain the executor's abort/restart safety path and verify bounded behavior under synthetic pressure.

Fewer passes do not divide all lookup work by the old pass count. The existing acquisition checks the hash range before probing, and composite kernels skip folds on rejected handles. Each group's table work already belongs to one pass; rescans repeat input reads, decoding/key construction and range hashing. Larger surviving tables can also increase cache misses. The measured scan and merge totals therefore do not support a sevenfold prediction. Only the prescribed paired comparison can establish payoff.

## Replaying a memoed pass count under the bounded share

A completed scan memoes its exact group count and pass count, and a later execution replays that pass count while each pass stays within twice the current budget. That multiple was calibrated for the shared quarter share, where twice the budget is at most half the headroom. Under the bounded three-quarter share it is unsafe: with q32's memo of (100,007,737 groups, 2 passes), an execution whose headroom has fallen to 4 GiB reads a budget of 28,760,941 groups, and replaying two passes sets the pass budget to 52,546,486 groups — the 50,003,869-group largest pass plus the skew margin — charging 5.48 GiB of tables (at 112 B) into that 4 GiB. Bounded plans therefore replay a memoed pass count only while each pass fits the current budget and otherwise take the passes the count implies: four at 4 GiB, at a pass budget of 28,760,941 groups charging 3.00 GiB. The shared share keeps its calibrated multiple. The paired legs did not run at the profile's 7,607 MiB: the candidate legs' own `[groupPlan]` lines (in `comparison.tar.gz`) record q32's bounded budget between 60,561,826 groups and the 2^26 cap, i.e. at least 8.4 GiB of effective headroom. At those budgets, as at the profile's, a 50,003,869-group pass fits the current budget outright, so both policies plan the same two passes for q32 and this review fix changes no measured leg; `GroupPassesBudgetRefreshTest` fixes the sequence as planning arithmetic without a 100M run.

The bounded gate has one more precondition, fixed at the document gate of the same review: the flat arms (numeric and packed-substring) receive the dispatcher's selection limit, which is the `Long.MAX_VALUE` sentinel for an uncapped query with a grouped distinct, a transformed key or a predicate tree. Under that sentinel every group is a winner, each partition selector is sized to its candidates and a retired partition copies every stripe out before its table is released, so the completed partitions are never released and the bounded premise does not hold. `SirixVectorizedExecutor.boundedSelection` now gates all four arms on a limit of at least one that is not the sentinel; `GroupHashRangePassTest` fixes both the gate and an uncapped disjunctive-predicate query served through the numeric arm without a bounded plan. The measurement is unaffected: all seven campaign queries are bounded by `LIMIT 10`, so every measured plan was and remains bounded.

## Disk spill: separate scope, no implementation here

Two passes still rescan. A general external aggregation path would let worker partial records stream once into radix partitions, spilling completed buffers when a query-owned memory reservation is exhausted. Serialize transient exact identity lanes, count/firstSeen, aggregate presence counts and values, and the source references required for winner materialization; exclude distinct sinks from this lever. This is temporary execution state, not a persisted database format change.

The executor needs query-scoped temporary-directory ownership, cancellation/error cleanup, an explicit file descriptor/buffer budget, and a final merge that reads one partition at a time, recursively repartitioning an oversized partition without rescanning the database. Under the rig its entire lifecycle stays within the exclusive lease and worktree output directory; database/corpora remain read-only. Apply the existing exact merge/overflow/empty-value semantics and preserve stable winner order across partitions.

At roughly 100 million q32 groups and current 88-byte payload records, a full set of temporary stripes is about 8.2 GiB plus framing; a write/read cycle moves about 16.4 GiB before compression, and worker duplicates can increase it. This is a substantial storage path (roughly 1–2 thousand production lines plus focused fault/pressure/semantic tests), not a budget constant adjustment. Its benefit is a one-scan bounded-memory safety valve at arbitrary cardinality, not an assumed speedup over in-memory two passes. It needs its own measurement allowance and attribution.
