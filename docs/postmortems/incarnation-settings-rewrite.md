# Bug: stale settings rewrite resurrects a destroyed incarnation

- Fix / issue: FMT-209, CONF-102 review R1.
- Symptom: rewriting settings with a retained configuration restores the identity of a destroyed
  timeline, so reissued revision numbers can share their former identity.
- Trigger: retain the configuration passed to resource creation, commit revisions, truncate through
  a session, then serialize the retained configuration. An independently deserialized configuration
  or the path-based restore publisher leaves the same stale-object risk.
- Root cause: serialization trusted the caller's cached identity. Creation bootstraps through a
  separate configuration; updating only the publishing instance cannot update every retained object.
- Why tests missed it: rewrite tests used the publishing instance or freshly deserialized settings,
  so they never exercised a configuration retained across an identity change.
- Regression test: `IncarnationIdConformanceTest.conf102StaleSettingsRewritePreservesTheTruncatedIncarnation`
  exercises JSON and XML creation, commits, session truncate and rewrites through both retained
  creation and deserialized configurations. FMT-209 supplies the oracle: preserve the installed
  post-truncate identity and adopt it in the serialized object; reopen must report that identity and
  the truncated head. Before the fix, the first rewrite restores the creation id by inspection.
  No pre-fix execution was run because this phase requires all fixes before verification.
  The existing missing/malformed-member tests also exercise refusal through serialization, and
  `ResourceConfigurationTest.fmt209SuccessfulPublicationSurvivesSettingsRewrite` covers both publishers
  with a retained configuration. Focused command:
  `./gradlew :sirix-core:test --tests io.sirix.access.IncarnationIdConformanceTest --tests io.sirix.access.ResourceConfigurationTest`.
- Lesson: reconcile persisted identity at every full settings rewrite, before opening the writer;
  an instance-local publication update cannot make other cached configurations authoritative.
  See the fragment collected into [LESSONS.md](../LESSONS.md).
- Vision check: preserves the identity of persisted history without changing formats, defaults or
  per-node work. Only settings serialization reads the installed configuration before rewriting.

## Sonar quality-gate follow-up

- Symptom / trigger: PR #1507's Sonar analysis rejected the settings-rewrite fix with BUG
  `java:S3078`, lowering new-code reliability to C instead of the required A.
- Root cause: serialization copied between volatile fields in one expression, then read the live
  field again for JSON output. Volatile access does not make that sequence atomic or establish one
  selected value for the rewrite.
- Correction: select the installed id through its getter into a final local, or retain the creation
  draw for absent/empty settings, then use that same local for the plain assignment and JSON output.
  Validation and adoption still precede opening the settings writer. This adds no locking or
  concurrent-publication guarantee.
- Regression evidence: the reported Sonar result supplies the pre-fix static-analysis failure;
  the existing conformance and publication-rewrite tests above supply the FMT-209 / CONF-102
  behavioural oracle. They reject resurrected ids after JSON/XML truncate or either publisher,
  including publication failures, and preserve unsigned boundary values and invalid-settings refusal.
  This syntax correction does not warrant a source-inspection test or a changed acceptance oracle.
- Lesson: use one selected identity throughout a settings rewrite, rather than rereading mutable
  state at the output boundary; see the existing lesson fragment.
- Verification: Spotless passed for all six touched Java files. The five touched test classes and
  core work-budget suites passed (530 tests, no failures or skips), as did the isolated prerequisite
  suites. Focused PIT analysis of serialization's id selection killed all three generated mutants
  with no uncovered mutants. The hosted Sonar quality-gate rerun remains with the outer executor.
