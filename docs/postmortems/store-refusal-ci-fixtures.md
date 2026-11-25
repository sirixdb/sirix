# Bug: store refusal exposed fixture reuse in CI

- Fix / issue: PR #1476; CI run 45492035335.
- Symptom: query tests failed on Linux, macOS and Windows with SIRIXDBF0006;
  PIT could not complete its unmutated baseline.
- Trigger: a helper or another test stores into a name its earlier invocation created.
- Root cause: fixtures relied on store implicitly deleting an existing database. Under
  the decided refusal contract, independent examples must start with a free resource name;
  reads of an existing fixture must reuse it, and deliberate database recreation must
  explicitly remove it. This applies to repeated assertions, parameterized shell modes,
  nested documentation examples, executor comparisons and custom database names.
  Separately, REST PIT selected Keycloak integration tests in a lane without Keycloak.
  Its baseline must use the same `offline` tag contract as `offlineTest`.
- Why tests missed it: the old destructive store behavior supplied accidental cleanup.
  The broader query and mutation CI lanes exposed the stale fixtures.
- Regression tests: the existing failing AutoWiredExecutorTest, MainTest,
  ProjectionIndexFunctionTest, VectorizedAggregateExactnessTest, ArchitectureDocQueryTest,
  OptimizerBehavioralE2ETest, JsonDBObjectFieldLookupTest, NumericComparisonRegressionTest
  and ShellPromptTest cases retain their answer/error oracles. Run them with
  `./gradlew :sirix-query:test --tests '<class or method>'`. Run REST PIT with its
  selected test patterns to verify that its baseline admits offline authorization tests.
- Lesson: [fixture ownership and mutation prerequisites](../lessons.d/store-refusal-ci-fixtures.md).

The existing store/load refusal tests remain the oracle for SIRIXDBF0006 and unchanged
history. Fixture cleanup must not relax that behavior or restore implicit replacement.
Vision check: this preserves immutable history and exact query answers; no new default.

Local evidence: the pre-fix selection ran 44 cases with 31 failures; the post-fix selection
ran 82 cases with no failures, including every originally failing case, both projection
predicate variants and both StoreIntoExistingDatabaseTest suites. The post-fix command uses
`:sirix-query:test --tests '<class or method>' -PtestHeapMin=1g -PtestHeapMax=3g`.
Kotlin API/CLI and PIT tooling passed 77 tests (four skipped); the Python mutation tooling
passed 266 tests. A REST PIT probe selecting both AuthNegativeTest and AuthBodyHandlerTest
completed its offline baseline and executed mutations with AuthBodyHandlerTest coverage.

The broader local query run stopped with exit 137 after 1,129 tests. Its 36 test failures
were a separate load-registration ordering problem: initializing Brackit's JSONFun first
selects Brackit's three-argument load signature (boolean third argument). An executable
registration probe reproduces that result with both the base JNFun and the PR JNFun;
this fix does not change registration. Logs and probe results are retained under
`build/ci-fix-evidence/` in the worktree. This is not evidence of a green full-suite run.
