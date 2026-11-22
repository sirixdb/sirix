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
