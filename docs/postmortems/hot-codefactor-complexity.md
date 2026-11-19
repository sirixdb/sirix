# HOT commit and fold complexity (Refs #1452)

- Symptom: CodeFactor rejects https://github.com/sirixdb/sirix/pull/1460 for three complex methods: page-reference
  commit (17), compatible-child preparation (23), and recursive normalization (29).
- Trigger: the normalization/outer-height change adds branches to already large
  methods. This is a maintainability failure, not a newly observed data failure.
- Root cause: cold reference resolution, fold eligibility/assembly, and recursive
  normalization each combine distinct decisions in one method.
- Why tests missed it: the existing behavior tests assert routing, persistence and
  ownership. Method complexity is a static-analysis responsibility; CodeFactor
  identified the maintainability regression. No source-presence test substitutes
  for that analyzer or for the runtime regressions below.
- Required behavior: extraction must preserve all existing observable behavior,
  allocation budgets and routing decisions. No analyzer configuration changes.

## Invariants and derived verification (before implementation)

1. Page commit must resolve or reject every child before parent serialization.
   Cover missing-log overflow/side-map references, stale non-durable claims,
   spilled pages, closed CoW references, ordinary dirty pages, disk-key/hash
   propagation, and the recursive HOT normalization guard. Existing storage/HOT
   commit, spill and revision regressions exercise the public persistence API.
2. Fold preparation must accept exactly the same representable routing
   compositions. Both insert folding and commit normalization share this boundary.
   `HOTCompatibleChildFoldTest` independently compares composed and two-stage
   routing across every slot, both directions, sparse partials, overlapping
   columns, 32/33-bit boundaries and SingleMask/MultiMask limits.
3. Normalization must mutate only owned pages, preserve leaves/identities, repair
   exact maximum heights and consume boundary metadata before another load can
   evict it. `HOTCommitNormalizerTest` covers folds, full-parent refusals, opaque
   clean children, eviction, stale ownership, unresolved children, cycles and depth.
   `HOTCommitWorkspaceTest` and failure/reuse assertions cover scratch lifetime;
   exceptions must still mark rollback-only and clear all borrowed references.
4. No-work/refused paths allocate zero workspace/output; successful folds and
   publications retain the exact pinned budgets. Use the isolated
   `HOTCommitAllocationBudgetTest`, plus fresh compiled-shape captures and the
   allocation re-measurement against published `9732747e`.

The four-block and 24-block latency experiments below are supplementary due
diligence for the CodeFactor refactor, not delivery acceptance gates. The required
re-measurement is allocation only. Their recorded HOLD/PASS classifications use
each experiment's fixed analysis rule and do not add requirements to this change.

These tests use routing, persisted state, explicit malformed graphs, and allocation
counters as oracles; no source-text test is added for a behavior-preserving refactor.
The wrong behaviors they reject include changed fold eligibility, dangling committed
children, stale heights, dirty-cycle recursion and new no-fold allocation.

## Mutation-derived coverage follow-up

Fresh PIT exposed untested cold branches and an actual-depth boundary hidden by
valid stored heights. `PageReferenceCommitTest` derives expected offset/hash,
image-release and reject-before-write behavior from the page-reference commit
contract; it rejects dangling stale claims, serialization of pending writes,
lost zero-hash presence and retained closed images. A 65-frame dirty chain with
understated metadata must fail with the controlled depth exception and clear
scratch. The full-parent test pins one log lookup per clean child: attempting a
fold when 32 children already fill the block adds useless ownership lookups.
No production behavior is changed by this test strengthening.

## Results — supplementary 24-block follow-up PASS

The CI fixer left the refactor uncommitted when its invocation budget expired.
The outer worker preserved and reapplied the patch to published `9732747e`; all
three changed production sources match the frozen measured candidate. No
CodeFactor waiver is accepted; the prepared refactor requires pipeline
revalidation and a fresh remote CodeFactor result before delivery.

- Compilation with Error Prone and formatting succeeded. The initial focused run
  passed 162 tests; the complete verification work-budget command passed 733 tests
  across 74 suites, including its isolated forks. Both had zero failures.
- The added public-commit and normalizer tests passed (9 and 18 cases). The final
  PIT coverage baseline also passed after strengthening the zero-log-key fixture.
- Scoped PIT: **179 mutants, 164 killed, 10 survived, 5 uncovered**. Initial PIT
  killed 145; the new assertions killed 19 more. Every residual has an individual
  [disposition](evidence/hot-codefactor-complexity/mutation-dispositions.md),
  separating equivalent/unreachable cases from inherited compatibility/concurrency
  coverage debt. This is not a claim of complete mutation coverage.
- Local Lizard complexity on the immediate published baseline versus this patch:
  page-reference commit **20 → 11**, compatible-child preparation **22 → 12**,
  recursive normalization **29 → 11**. Every extracted helper is at most 12.
  The supplied provider diagnostics were 17/23/29 at their reported snapshot;
  local and provider figures are not presented as the same analyzer run.
- Fresh compiled-shape captures cover the three kernels and all eight helpers,
  including helper inlining into C2. Kernel bytecode sizes are 438 → 275,
  572 → 360, and 633 → 238 bytes respectively. The raw report preserves all
  recompilations and exercised profiles; synchronous diagnostic timing is ignored.
  Previous compiled-shape hashes are not used as proof of this patch.
- The eight isolated `HOTCommitAllocationBudgetTest` cases passed unchanged.
  Workspace acquisition, no-work/no-fold paths and refused folds remain zero-byte;
  the tested successful output graphs retain their exact 240/352-byte budgets.

### Stack-attributed allocation confirmation

Each arm/case uses the unchanged task-proof fixture, 30 seconds real warmup,
50 warmup operations and all 40 retained operations. Baseline production classes
are frozen from published `9732747e47fc549960256af9d4516644badfda1e`, compiled in
this worktree before extraction. Candidate source and class hashes still match
the frozen snapshots. Paired classpaths have identical ordered dependencies and
exactly one production snapshot; equal-length `before`/`after_` paths remove the
old 54-versus-53-entry asymmetry. No other source checkout supplies classes.

| Case | Folds | Exact owned graph bytes | Publications | Normalization before → after | Unchanged bound | Workspace |
|---|---:|---:|---:|---:|---:|---:|
| commitDesc4096 | 3 | 784 | 2 | 1,152 → 1,152 B | 1,152 B | 0 B |
| commitDesc50000 | 41 | 13,392 | 24 | 17,744 → 17,808 B | 17,808 B | 0 B |

The publication bound remains **32 B PageContainer + 152 B existing cache
retirement per publication**. The 50k increase is exactly two 32-byte
`ConcurrentHashMap.ReservationNode` allocations in existing cache retirement;
it consumes the original envelope. No allocation bound was widened. All 160
retained profiled operations meet the fixed bounds and have zero timed GC.

Whole-process allocation is reported separately: direct-counter means were
233,518.6 → 233,538.4 B for 4096 and 267,582.2 → 267,650.4 B for 50k. The
zero-interval native counter means were 233,518.6 → 233,537.6 and
267,553.8 → 267,621.4 B respectively. These totals include unrelated runtime and
background variation; they are not relabelled as workspace or fold-output bytes.
The profiler's escaping controls passed on both platform and virtual threads.

### Supplementary paired latency follow-up — HOLD

The preregistered 4 ABBA blocks completed: 16 fresh JVMs, five cases, 30 seconds
real warmup plus 50 JMH warmup batches and all 36 retained measurements, default
asynchronous C2, CPU0 and an exclusive inherited-lock window through `heavy.sh`.
The experiment's fixed analysis rule was point ≤ +1%, lower 95% log-ratio bound ≤ 0, zero timed GC;
the four-block Student t critical is 3.182446305284263 (df 3).

| Case | Units | Before | After | Paired change |95% interval| Bytes before/after |Median paired byte delta|Timed GC|Experiment classification|
|---|---|---:|---:|---:|---|---|---:|---:|---|
|insertAsc4096|ns/key|1018.3827|1005.2103|-1.304%|[-6.248%,+3.901%]|584.946289/584.966064|+0.008301|0|PASS|
|insertDesc4096|ns/key|1262.0371|1231.6227|-2.387%|[-7.679%,+3.207%]|618.293945/618.298828|+0.001953|0|PASS|
|insertDesc50000|ns/key|1626.4909|1613.9221|-0.754%|[-3.903%,+2.499%]|812.245520/812.251040|+0.000320|0|PASS|
|commitDesc4096|ms/commit|4.6881|4.7702|+1.466%|[-11.714%,+16.613%]|241063.250000/241143.333333|+150.500000|0|HOLD|
|commitDesc50000|ms/commit|15.2528|15.8635|+3.417%|[-10.397%,+19.361%]|253183.583333/253307.888889|+112.055556|0|HOLD|

Both commit cases exceed the fixed +1% point-estimate limit. Their intervals
include zero: this is not a statistically conclusive slowdown, but it also does
not satisfy the experiment's fixed analysis rule. All insert cases pass and timed GC
is zero throughout. Allocation subsystem budgets are checked separately above;
whole-process byte variation in the table is not attributed to a new workspace.
No samples were discarded. The supplementary unchanged-candidate trial below follows this result without pooling it.

The disk preflight initially held at approximately 51 GiB. After recovery above
60 GiB and three quiet-admission observations, the single run began at
`2026-11-19T00:52:05Z` and completed at `2026-11-19T01:39:25Z`. The harness
continued after its controlling CI agent hit its invocation limit. The outer
worker recovered all 16 results and analyzed the complete set; it did not restart
the experiment. Frozen source identity and 5,876 runtime-file hashes were checked
after completion. The archived analysis retains the full counters and raw data.

An additional zero-slot test/PIT follow-up completed after the earlier disk drop;
those results are correctness evidence, not a claim that the disk precondition
held for every local JVM. No timing JVM started below the floor.

The four-block result remains **HOLD**, and CodeFactor has not analyzed this
unpublished refactor. This result is retained separately. A subsequent
preregistered 24-block trial used the unchanged candidate because the four-block
intervals are too wide to resolve commit timing. That supplementary trial uses the same
analysis rule and df 23; it is not pooled with this HOLD. See the
[24-block protocol](evidence/hot-codefactor-complexity/protocol-24-blocks.md).
Its completed results follow; production sources and measured bytecode were unchanged between trials.

### Supplementary 24-block trial — PASS

The 24 ABBA blocks explored precision after the four-block
HOLD as supplementary due diligence. An initial attempt was interrupted on a scheduling instruction after
58 of 96 JVMs; its partial raw data is retained separately and was not analyzed
or used to tune the candidate. The fresh retry completed
all 96 JVMs. It ran directly through `timing-window.sh` on CPU0, under the
exclusive timing lock, with three quiet-admission observations and at least
60 GiB free before admission and each JVM. This scheduling override replaces
the earlier nested wrapper; ordinary JVM verification still used `heavy.sh`.

The baseline remains published `9732747e`; all 1,054 production source and
1,854 local production class hashes match the frozen candidate. Ordered
classpaths differ only in their equal-length production snapshot path. The
runner checked frozen runtime/source hashes before every JVM and at completion.
The experiment's fixed analysis rule was paired point estimate ≤ +1%, lower 95% log-ratio
bound ≤ 0, and zero timed GC, using Student t with df 23. No trial was pooled,
no samples were discarded and no production tuning followed timing results.

24 ABBA blocks per case;96 fresh JVMs;five cases. Fixed95% block-log-ratio t interval,df23. All36 measured samples retained,30s real warmup plus50 JMH warmup batches. Commit allocation uses JVM-wide allocated bytes because commit runs on virtual/background threads; insert allocation uses caller-thread bytes. GC-profiler fixture totals are reported separately.

| Case | Units | Before | After | Paired change |95% interval| Bytes before/after |Median paired byte delta|Timed GC|Experiment classification|
|---|---|---:|---:|---:|---|---|---:|---:|---|
|insertAsc4096|ns/key|1025.6665|1028.4748|+0.301%|[-1.149%,+1.773%]|584.467719/583.455566|-0.011719|0|PASS|
|insertDesc4096|ns/key|1259.0910|1249.6007|-0.708%|[-2.247%,+0.856%]|617.816081/616.808594|-0.025391|0|PASS|
|insertDesc50000|ns/key|1624.3371|1625.2312|+0.054%|[-0.518%,+0.630%]|811.749684/810.741279|-0.002080|0|PASS|
|commitDesc4096|ms/commit|5.0922|5.1300|+0.513%|[-3.598%,+4.799%]|240589.731481/240647.152778|+107.666667|0|PASS|
|commitDesc50000|ms/commit|16.1277|15.8614|-1.555%|[-8.853%,+6.328%]|252884.675926/252859.273148|-58.833333|0|PASS|

Overall experiment classification: PASS

Profiler fixture-inclusive allocation and GC totals:
- insertAsc4096: 670.880216->669.835638 B/key; all GC5827,forced requests3456,timed0.
- insertDesc4096: 690.887573->689.892834 B/key; all GC5749,forced requests3456,timed0.
- insertDesc50000: 817.854850->816.847212 B/key; all GC5990,forced requests3456,timed0.
- commitDesc4096: 3281386.361111->3277204.421296 B/commit fixture; all GC5036,forced requests3456,timed0.
- commitDesc50000: 41280518.745370->41230069.111111 B/commit fixture; all GC5918,forced requests3456,timed0.

All five cases meet the experiment's fixed analysis rule. Commit intervals remain wide; these results
do not establish equivalence within 1% or a precise speedup. Whole-process
allocation is separate from the unchanged exact subsystem budgets above.

Fresh local verification after reapplying the patch passed 123 focused tests
and 8 isolated allocation tests. The full work-budget command succeeded; its
main XML reports contain 326 core, 327 query (5 existing optional skips), and
3 MCP tests, with zero failures/errors. Isolated task results and full logs
are retained rather than summed into these main-report counts. Fresh compiled
shape analysis retains 71 profiles, including 33 C2 profiles: four helpers have
standalone C2 profiles and four have hot inlining in exercised C2 kernels.
Early C1 refusals are retained; this is not a universal inlining claim.

CodeFactor remains a required remote gate after publication; local complexity
and performance evidence does not waive it.

Evidence: [manifest and replay notes](evidence/hot-codefactor-complexity/README.md).
Diagnostic setup failures (local heap minimum, missing JMH registry, the 32-child
probe factory, and a transient unfrozen test classpath) were corrected before any
paired latency run. The production snapshots were not tuned after observing
allocation or compiled-shape results.

## Lesson

Extract a complete cold decision at its shared boundary, and remeasure changed
bytecode rather than treating a method extraction as performance-neutral. See
[LESSONS](../LESSONS.md).
