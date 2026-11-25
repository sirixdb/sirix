# Lifecycle CI source hygiene

- Fix / issue: PR #1353, CodeFactor CI repair.
- Symptom: the preceding CI summary reports two unused imports and a benchmark
  package-layout finding, alongside eleven complexity findings. The provider's
  diagnostic page currently returns HTTP 403; its individual complexity sites
  cannot be confirmed from that summary.
- Trigger: analyzing the lifecycle test fixtures and the added standalone JMH fixture.
- Root cause: test fixture revisions left `IntConsumer` unused in
  `DatabaseNotificationLifecycleTest` and `TimeoutException` unused in
  `NodeTransactionAbortLifecycleTest`. The benchmark was placed directly beneath
  its source root rather than under its declared `io.sirix.benchmark` package.
- Why tests missed it: Java permits unused imports and compilation from an explicit
  source set does not require package-matching directories. Runtime tests therefore
  do not reject either defect.
- Correction: remove the two imports and move the benchmark into the matching package
  directory. The benchmark source bytes, source-root configuration and JMH selector
  are unchanged. No assertions, production code, sampling caps or mutation
  dispositions are weakened.
- Regression verification: compile the lifecycle tests and benchmark with their real
  Gradle consumers and check formatting. A source-string test would not establish
  behavior and is deliberately not added for this source-only repair. Compilation
  is not evidence for the still-outstanding full PIT or main/head compiled-path
  acceptance work.
- Local result: `:sirix-core:compileTestJava :sirix-core:spotlessJavaCheck` and
  `:sirix-benchmarks:compileJmhJava` with the existing lock-benchmark init script
  both passed through the required admission wrapper and container-task guard.
  Both recorded task graphs contained no forbidden tasks. No benchmark was executed.
- Lesson: [source hygiene and executable verification](../LESSONS.md#separate-source-hygiene-from-behavioral-verification).

The missing qualified mutation accounting is restored separately in
[lifecycle mutation accounting](../lifecycle-mutation-accounting.md). That note
retains raw timeout/survivor statuses and the unresolved recovery survivor; it does
not turn archived evidence into current-head proof. PR publication and readback
remain the outer executor's responsibility.

## Error Prone interrupt-branch follow-up

- Fix / issue: PR #1353, Compile and package run 45096765238, job 137438180345.
- Symptom / trigger: `:sirix-core:compileTestJava` rejects the interrupt branch of
  `ConcurrentAxisLifecycleTest.assertCancellationFailure` with `ReturnValueIgnored`.
- Root cause: the failure-path fixture called `hasNext()` for its interruption effect
  without explicitly consuming or discarding the boolean result. The compiler's return-value
  contract applies even when the test expects the invocation to throw.
- Why tests missed it: runtime-only validation did not enable the hosted Error Prone gate.
- Correction: explicitly discard the result in a local variable. Preserve the operation,
  shared failure assertions, interruption restoration, reader ownership, latches, names,
  deadlines and all JSON/XML close/reset/interrupt cases with both cleanup-failure forms.
- Regression verification: the hosted log is the before-fix compiler failure. Run
  `:sirix-core:compileTestJava` with the unchanged `-PerrorProne=true` configuration,
  `ConcurrentAxisLifecycleTest`, and `:sirix-core:spotlessJavaCheck` under the required JDK
  and resource admission. This test-source correction changes no production logic and
  supplies no replacement for pending final-head mutation, compiled-path or latency evidence.
- Local status: `git diff --check` passed. Compilation, focused regression and Spotless
  remain blocked because the required task Temurin JDK's absolute path was not supplied
  or referenced in the worktree/evidence setup. The exact path was requested; no host
  tool search or GraalVM substitution was performed. No passing Temurin evidence is claimed.
- Lesson: [source hygiene and executable verification](../LESSONS.md#separate-source-hygiene-from-behavioral-verification).
