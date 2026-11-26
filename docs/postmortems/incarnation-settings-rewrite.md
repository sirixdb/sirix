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
