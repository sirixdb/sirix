# Bug: native feasibility checks miss the public terminal path

- Fix / issue: [I2](https://github.com/sirixdb/sirix/issues/1437), PR #1451.
- Symptom: PIT reports an uncovered removal of the public shutdown delegation;
  Sonar reports 55.6% new-code coverage and an unpinned workflow action;
  CodeFactor reports missing package declarations in both native fixtures.
- Trigger: run the PR's automatically selected mutation tests and static analysis.
- Root cause: the JVM regression calls the drain helper directly, bypassing both
  production delegation layers. The standalone fixtures and new workflow did
  not follow the same namespace and immutable-dependency rules as library code.
- Why tests missed it: the native C smoke exercises the public hook, but its
  coverage is not JVM JaCoCo/PIT coverage. Testing the helper cannot detect a
  removed call in either wrapper.
- Regression contract: NATIVE-406 requires the public hook to reach the engine's
  append and serialization pools in that order. A JVM test substitutes owned
  executors at that terminal infrastructure boundary, then runs the real drain
  and checks queued work completes and both pools terminate. Removing either
  delegation must fail. Null-input and repeated-drain behavior remain covered by
  `SnapshotExecutorShutdownTest`. All three actions in the new workflow must
  resolve to immutable commits. Both smoke fixtures must compile in a named
  package while exporting the same explicitly named C entrypoints.
- Regression test: `StorageEngineWriterFactoryTest` plus the existing native C
  smoke, workflow consumer (`actionlint`), and scoped PIT; results below.
- Lesson: [exercise public lifecycle adapters in the selected JVM tests](../LESSONS.md).

## Verification (2026-11-17)

- Original [CI mutation artifact](https://github.com/sirixdb/sirix/actions/runs/44138804508/artifacts/14354436437):
  2/3 changed-line mutants killed; the public delegation was `NO_COVERAGE`.
- `:sirix-core:test --tests io.sirix.access.trx.page.StorageEngineWriterFactoryTest
  --tests io.sirix.access.trx.page.SnapshotExecutorShutdownTest`: all three tests
  pass, including with JaCoCo enabled. The public-hook omission now fails its
  append-termination assertion under PIT.
- `python3 .github/scripts/pr_mutation.py run --output
  build/ci-diagnosis/pit-fixed --seconds 600 --blocking`: passes, 3/3 changed-line
  mutants killed, none surviving or uncovered, and no unmapped changed sources.
  The 108 sampled mutants on unchanged lines remain separately reported advisory
  debt; their disposition is unchanged by this repair.
- JaCoCo reports all nine changed executable lines covered (previously 5/9),
  including both public delegation layers. The default local report task has a
  pre-existing input issue: it includes a shared-arena test-results directory as
  execution data. A local init script selects only the emitted `build/jacoco/*.exec`
  files for this report. The hosted Sonar lane uses `jacocoMergedReport` instead;
  no production build configuration was changed for this local reporting issue.
- `actionlint .github/workflows/native-library-smoke.yml` and
  `shellcheck tests/native/run-smoke.sh`: pass. All three action SHAs were resolved
  from their existing version tags through GitHub's refs API.
- With `GRAALVM_HOME` set to the GraalVM 25.0.3 installation on PATH,
  `tests/native/run-smoke.sh all "$PWD/build/ci-diagnosis/native-packages"`:
  passes for both core and query. Each shared library was called twice by C with
  `PATH=/nonexistent`, returned 42 after reopen (also through JSONiq for query),
  passed all 16 cache shapes, loaded no JVM, spawned no executable, and returned
  zero from snapshot shutdown and isolate teardown. The C compiler consumed the
  generated headers with the existing entrypoint names after packaging the Java
  fixtures. Builders used two threads and a 4 GiB heap.

Local logs and reports are under `build/ci-diagnosis/`. Hosted analyzer verdicts
must be refreshed by the outer CI executor after publication.

## CI follow-up: formatter contract

- Symptom / trigger: `Compile and package` rejects the newly added public-hook
  test at `:sirix-core:spotlessJavaCheck`.
- Invariant / scope: every Java file changed since `origin/main` must match the
  configured Eclipse formatter, including all try-with-resources continuations
  in the shutdown regressions and production adapters.
- Root cause: the public-hook test was added with continuation indentation one
  space deeper than the formatter permits. Behavioral and mutation tests do not
  check formatting; the formatter was not rerun after adding this test.
- Regression / oracle: execute the real Spotless consumer with the repository's
  formatter profile. `./gradlew :sirix-core:spotlessJavaCheck` fails before the
  repair and must pass afterward; `./gradlew spotlessCheck` checks sibling
  modules. Apply formatting without changing resource order, mock delegation,
  shutdown calls or assertions. No executable logic changes, so there are no
  new logic mutants to exercise.
- Lesson: rerun the configured formatter after the final test edit, including
  tests introduced during a CI repair; see [Lessons](../LESSONS.md).
- Verification (2026-11-17): the focused check reproduced the hosted failure;
  `:sirix-core:spotlessJavaApply` changed only the three reported indentation
  lines, then `./gradlew spotlessCheck --console=plain` passed all eight modules
  (16 tasks executed). `git diff --check` passed.
- Separate unresolved check: GitHub's CodeFactor status for commit
  `273c67d3371aaf807f9eec7d4e4e4716e70e8b70` reports two issues. Its linked
  [detail page](https://www.codefactor.io/repository/github/sirixdb/sirix/pull/1451)
  returned HTTP 403 through direct retrieval and Chrome; GitHub exposes no
  corresponding check-run annotations. The issue details remain unavailable,
  so this formatting repair does not claim to resolve CodeFactor.
