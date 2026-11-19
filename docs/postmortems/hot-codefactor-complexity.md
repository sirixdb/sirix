# HOT commit and fold complexity (Refs #1452)

- Symptom: CodeFactor rejects PR #1460 for three complex methods: page-reference
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
   authorized four-block ABBA follow-up against published `9732747e`.

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

## Results — latency follow-up in progress

All source changes stay in this CI-phase worktree. No commit, push, PR edit,
merge, pipeline-control action or CodeFactor waiver was performed.

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

### Required paired latency follow-up

The preregistered follow-up is **4 ABBA blocks / 16 fresh JVMs / five cases**,
30 seconds real warmup + 50 JMH warmup batches + 36 retained measurements, default
asynchronous C2, CPU0 and an exclusive inherited-lock timing window through
`heavy.sh`. Its decision remains point ≤ +1%, lower 95% log-ratio bound ≤ 0,
zero timed GC, Student t critical 3.182446305284263 (df 3). No gate outcome is inferred from diagnostic timings. The fixed paired run began
after fresh quiet admission and recovery of the disk floor.

At preflight, free disk fell to approximately **51 GiB**, below the required
**60 GiB**. The entire worktree is approximately 1.5 GiB, so its disposable
outputs cannot restore the floor. External files were neither searched for
cleanup nor modified. No timing-gate JVM was started below the floor. An
additional zero-slot test/PIT follow-up completed after the disk drop; its
results are correctness evidence, not an assertion that the disk precondition
was maintained for every local JVM throughout the session.

Disk space subsequently recovered above 60 GiB. Three fresh quiet-admission
observations passed, and the single exclusive comparison began at
`2026-11-19T00:52:05Z`. All 16 JVMs must complete before the decision is evaluated;
results are pending. The outer executor owns publication and the actual remote
CodeFactor green verdict. Prior timing provenance and `Refs #1452` remain intact.
Frozen production snapshots and replay mechanics remain in `build/codefactor/`.

Evidence: [manifest and replay notes](evidence/hot-codefactor-complexity/README.md).
Diagnostic setup failures (local heap minimum, missing JMH registry, the 32-child
probe factory, and a transient unfrozen test classpath) were corrected before any
paired latency run. The production snapshots were not tuned after observing
allocation or compiled-shape results.

## Lesson

Extract a complete cold decision at its shared boundary, and remeasure changed
bytecode rather than treating a method extraction as performance-neutral. See
[LESSONS](../LESSONS.md).
