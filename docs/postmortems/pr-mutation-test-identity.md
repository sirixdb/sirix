# Bug: PR mutation test identity and uncovered acceptance

- Fix / issue: review findings R1 and R2 on `fm/sirix-ci-pit-scope`.
- Symptom: PIT omitted `KeyValueLeafPageTest` because its source is named
  `KeyValueLeafTest.java`; a completed report with only uncovered mutants could pass blocking.
- Trigger: select `KeyValueLeafPage.java` before test compilation, or consume a completed PIT
  report containing `detected="false" status="NO_COVERAGE"` and no survivors.
- Root cause: test discovery substituted source paths for JVM type identity at the shared
  boundary used by named, related and package lookups. Separately, the timeout repair changed
  the clean criterion from every mutant detected to no `SURVIVED` status, accepting untested
  code. Selection bounds and acceptance policy are independent concerns.
- Why tests missed it: test fixtures were empty files whose stems always matched the assumed
  class names, and the uncovered-report test explicitly blessed the weaker acceptance policy.
- Regression test: `SelectionTest.test_declared_test_identity_reaches_every_resolution_path_without_compilation`
  exercises declared Java/Kotlin names, declared packages, all three lookup paths and the
  emitted Gradle argument. The oracle is the language declaration, independent of filenames
  and compilation caches. `test_only_top_level_declarations_select_tests` adds comments,
  escaped strings, text blocks, nested classes and multiple top-level classes.
  `ReportingTest.test_only_complete_fully_detected_reports_are_clean` and
  `test_completed_uncovered_report_fails_blocking_but_keeps_advisory_reporting` use PIT's
  `detected` contract and the required every-mutant-detected policy, observing exit status,
  persisted results and summary output. Each regression rejects the pre-fix implementation.
- Verification: focused `test_pr_mutation.py` suite, pre-fix replay of the four regressions,
  and targeted in-memory mutations of discovery and clean-result predicates. No repository-wide
  test or lint gate is part of this review phase.
- Lesson: [preserve declared identities and acceptance policy](../LESSONS.md).
