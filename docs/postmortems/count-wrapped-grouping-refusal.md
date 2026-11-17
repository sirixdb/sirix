# Bug: count-wrapped grouping bypasses refusal recovery

- Fix / issue: PR #1352 review R1.
- Symptom: counting nested sparse groups throws `VectorizedRouteRefusedException`;
  admitted grouping can return group objects instead of their cardinality.
- Trigger: `count(for $r in jn:doc('db','res').rows[] let $d := $r.dept
  group by $d return {"dept":$d,"count":count($r)})` over
  `{"rows":[{"dept":"Eng"},{"dept":"Eng"},{}]}` must return `2`.
- Root cause: Brackit's function compiler intercepts `count(PipeExpr)` before the
  physical pipeline strategies. It substitutes the grouping expression for the
  whole call, losing both the outer count and the strategies' refusal recovery.
- Behaviour / invariant: built-in count over grouping counts the complete grouping
  result; refusal retains generic grouping semantics, admitted grouping retains
  priority, and ordinary errors propagate. This covers single/composite keys,
  nested/top-level sources, all sequential/parallel factories and cold reopen.
- Why tests missed it: grouping differential queries consumed the groups directly;
  field-count controls did not exercise count wrapping a grouping pipeline.
- Regression tests (derived before implementation): extend the release differential
  harness with count-wrapped sparse single/composite grouping and admitted dense
  controls for every compilation factory, warm and reopened. Compare with forced
  generic execution and independent cardinalities (`2` single/dense, `3` composite
  sparse), requiring actual SPI execution and the expected refusal/admission.
  Repeated input keys distinguish group cardinality from source-row cardinality.
- Fix: resolve the outer count normally at Sirix's function boundary and compile its
  grouping argument through the selected pipeline strategy. This removes the
  invalid whole-call substitution and reuses existing grouping refusal recovery.
- Verification plan: one focused end-of-round run of the release/fallback suites and
  query work budgets. The outer pipeline owns mutation, lint and full test gates.
- Lesson: audit function-level interception as well as physical strategies when an
  SPI introduces refusal; preserve outer scalar operations. See [Lessons](../LESSONS.md).

## Focused verification evidence

- `build/review-count/verification.gradle` compiles the original HEAD translator into
  a shadow directory and runs only the two new parameterized tests with it first on
  the classpath. All 20 cases fail, across all five factory configurations and both
  key shapes. Sparse cases produce `bit:BIDY0300`; dense cases return group objects
  instead of the scalar cardinality. Both warm and reopened assertions fail.
  Results: `build/review-count/baseline-results/`.
- The corrected class passes all 195 selected tests without skips: 125 release
  correctness cases, six fallback-expression tests, and 64 query work-budget cases
  in `IndexRoutedGroupWorkBudgetTest`, `ProjectionQueryWorkBudgetTest` and
  `ProviderPurityWorkBudgetTest`. The single focused Gradle invocation also executes
  its existing mandatory isolated reader-lifetime and translator-threshold tasks.
  Results: `bundles/sirix-query/build/test-results/test/`;
  command and build evidence: `build/review-count/verification.log` and its init script.
- The original-class shadow also exercises deletion of the new interception bypass:
  all 20 new cases kill that deletion. This is a focused deletion check, not a PIT
  report; the dedicated outer mutation gate remains authoritative for other mutants.
- Final path trace: the resolved count function receives the pipeline expression
  compiled by the selected strategy. Supported grouping returns its groups for count
  to consume; refusal evaluates the retained generic grouping before count consumes
  it. The same bindings and static function context are retained. Non-grouping counts,
  count-distinct-only annotations, value aggregates and computed aggregates keep their
  existing paths. No helper or parameter becomes unreachable; no hot kernel or budget
  bound changes. All JVM work used the supplied heavy wrapper, bounded heaps and two
  workers, with 90 GB free before the build. No full repository test or lint suite ran.
