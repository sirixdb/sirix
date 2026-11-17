# Bug: parallel grouping loses generic fallback

- Fix / issue: PR #1352 review R2.
- Symptom: valid nested single-key and composite grouping queries throw
  `VectorizedRouteRefusedException` instead of returning all groups.
- Trigger: compile a nested sparse grouping without a covering projection through either
  `createParallel` overload or `createParallelWithMorsel`, then consume its results.
- Root cause: refusal recovery belonged only to the sequential physical strategy, although
  Brackit's block strategy admits the same grouping SPI. The compiler boundary must retain
  the selected strategy's generic evaluator for every legacy grouping substitution.
- Behaviour / invariant: an explicit grouping route refusal must evaluate the retained generic
  pipeline with the same bindings; supported routes retain priority and ordinary failures
  propagate unchanged. This applies to sequential, ordered/default/unordered parallel and
  parallel-with-morsel chains, single/composite keys, and warm/reopened resources.
- Why tests missed it: the release differential harness always created sequential chains.
- Regression tests (derived before implementation): expand the existing nested sparse
  string/numeric and composite fixtures across all public chain factories; add dense top-level
  single/composite controls. Compare against forced-generic query execution and require real
  SPI execution plus the expected refusal/admission before and after a cold reopen. A wrapper
  missing on any factory, skipped SPI, partial group or unnecessary refusal must fail.
  `VectorizedFallbackExprTest` independently checks error propagation, bindings and empty results.
- Verification plan: one focused round after all fixes, with a baseline compiler shadow to
  reproduce both nested failures, the corrected differential/fallback tests, relevant query
  work budgets and scoped PIT. No scan kernels or budget bounds change.
- Lesson: enumerate every physical strategy consuming an SPI before adding a refusal contract;
  pair negative-route cases with admitted controls. See [Lessons](../LESSONS.md).

## Verification and mutation dispositions

- `build/review-r2/verification.gradle` compiles the original `SirixCompileChain` into
  a separate class directory and runs the new nested fixtures with that directory first
  on the baseline test classpath. Both single-key types and composite grouping fail for
  all four parallel factory configurations, warm and reopened: 12 failures of 15 cases;
  the three sequential controls pass. XML: `build/review-r2/baseline-results/`.
- With the corrected compiler, all 295 focused tests pass without skips: the 84-case release
  suite, 129 typed grouping differentials, four source-scope differentials, six fallback
  checks, eight ordinary parallel queries, and 64 query work-budget cases across
  `IndexRoutedGroupWorkBudgetTest`, `ProjectionQueryWorkBudgetTest` and
  `ProviderPurityWorkBudgetTest`. Log: `build/review-r2/verification.log`.
- Scoped PIT covers the two physical strategies' `compilePipeExpr` methods and their shared
  `needsGroupingFallback` condition, excluding unrelated methods. Including the existing
  `ParallelQueryExecutionTest` in the follow-up kills the initially uncovered non-grouping
  return mutation. Final XML: `build/review-r2/pit-final/mutations.xml`; log:
  `build/review-r2/mutation-followup.log`. Ten mutants: eight killed, two survived,
  none uncovered or timed out. All three mutants in the new block adapter are killed.
- Survivor: `SirixPipelineStrategy.needsGroupingFallback`, line 84,
  `NegateConditionalsMutator`, instruction index 12. The pinned optimizer always sets the
  multi-key annotation before the optional legacy single-key annotation. Negating the
  legacy annotation cannot remove fallback from a compiler-produced grouping. For other
  pipes it can add a redundant wrapper. Retain the explicit legacy annotation for SPI
  compatibility; this is the same previously documented annotation redundancy, now shared
  by both physical strategies.
- Survivor: the same method, line 83, `BooleanTrueReturnValsMutator`, index 31. Always
  retaining generic fallback adds wrappers to non-grouping/no-executor pipelines, whose
  routed evaluators cannot emit the grouping refusal. The selected deterministic queries
  therefore preserve results and failures. This is a result-contract limitation of the
  scoped tests, not a claim of equivalent compilation work: the mutant can compile an
  unnecessary generic pipeline. The unchanged conditional avoids that extra work.
- Path audit: both parallel overloads and parallel-with-morsel install the block adapter;
  its inherited ordering configuration remains intact. Membership queries still select
  the Sirix sequential strategy. Each strategy compiles its original routed expression
  first and retains its own generic pipeline only for annotated grouping with an SPI.
  Supported grouping returns directly; explicit refusal evaluates that generic pipeline;
  unrelated exceptions and empty results retain the existing fallback-expression contract.
  No helper, branch or parameter became unreachable, and no row/page kernel or budget
  bound changed. All JVM work used the supplied heavy-job wrapper with more than 75 GB
  free on `/` before each Gradle invocation. Full repository gates remain with the executor.
