# Bug: incarnation publication diverges after a directory barrier failure

- Fix / issue: FMT-209 review, R1 and R2.
- Symptom: the settings contain a fresh incarnation id while the live configuration still exposes
  the old one; a subsequent settings rewrite can resurrect the old identity. Windows publication
  also claimed durability without a metadata barrier.
- Trigger: publish a fresh id, complete the settings rename, then fail directory open, force or
  close. On Windows, a crash after a successful return can lose the rename.
- Root cause: adoption belonged to the caller's successful return rather than the shared
  publisher's rename boundary. A rename changes the installed identity independently of whether
  the following durability barrier succeeds. Journaling was incorrectly treated as a Windows
  durability barrier.
- Why tests missed it: lifecycle and crash-order tests covered successful publication and
  interruption before destructive work, but did not inject a directory-channel failure after
  installation and then serialize the same live configuration.
- Regression test: `ResourceConfigurationTest.fmt209FailureAfterRenameKeepsTheInstalledId`
  injects directory open, force and close failures through the public publishers. Its oracle is
  FMT-209's publication failure contract: the installed settings id and live accessor must agree,
  the exception must propagate, unrelated settings must survive and serialization must preserve
  the installed id. The pre-fix instance publisher fails the live-id assertion by inspection;
  a pre-fix execution was not run because this review phase requires all fixes before verification.
  Companion tests cover pre-rename failure and ordinary publication through both overloads.
  Focused command: `./gradlew :sirix-core:test --tests io.sirix.access.ResourceConfigurationTest --tests io.sirix.access.IncarnationIdConformanceTest`.
- Lesson: adopt installed state at its publication boundary even when later durability work fails;
  propagate the durability error before destructive work. Record platform guarantees from actual
  barriers. See the lesson fragment collected into [LESSONS.md](../LESSONS.md). The Windows
  metadata gap this fix originally recorded was closed by TXN-616: the publisher now replaces the
  settings through `DurableFiles`, durable on return on every platform.
- Vision check: keeps identity consistent with persisted history without changing hot paths,
  query cost, supported formats or defaults; reports the existing durability gap honestly.
