# Bug: legacy envelope byte oracle inherited random record-page salts

- Fix / issue: https://github.com/sirixdb/sirix/pull/1354 integrates the strict readers from
  https://github.com/sirixdb/sirix/pull/1355.
- Symptom: `LegacyEnvelopeImagesTest` reported that an unknown extension survived rewriting
  kind 1. The first differing byte was 33: the nine-byte page prefix plus salt offset 24.
- Trigger: read an immutable pre-salt record-page image with an injected optional extension,
  then rewrite it under the newly default-on salt configuration and compare all bytes.
- Root cause: the byte oracle assumed deterministic writer defaults. The rewrite correctly
  removed the extension but initialized the old zero salt, so the assertion conflated salt
  generation with extension preservation. The salt and strict-reader branches passed separately.
- Why tests missed it: the immutable-image suite was introduced after the salt fixture audit.
  Running both features together exposed its implicit configuration dependency.
- Regression test: `LegacyEnvelopeImagesTest.everyBaseKindSkipsOptionalExtensionsAndRefusesRequiredOnes`
  failed at byte 33 before the fixture configuration correction. Enable deterministic mode only
  in this byte oracle. `PageKindTest.optionalExtensionsPreserveSaltAndRecordsWhileRequiredEntriesRefuse`
  separately verifies nonzero default salts, retained records and salts, extension removal on
  rewrite, eager/lazy reads, probe offsets and required-entry refusal in both body forms.
  `ResourceConfigurationTest` also covers modes, legacy defaults, omitted/reordered optional
  fields, nested extensions and strict unknown-field refusal together.
  The corrected focused suite passed all 133 cases; the preceding run passed all 314 work-budget
  cases without changing a bound. Reproduce the integration regressions with
  `./gradlew :sirix-core:test --tests io.sirix.page.LegacyEnvelopeImagesTest --tests io.sirix.page.PageKindTest --tests io.sirix.access.ResourceConfigurationTest`.
- Lesson: make byte-oracle determinism explicit at its configuration boundary, and keep a
  default-salted companion that checks records and the intended byte differences. No golden
  constant or immutable fixture changed; see the integration entry in [Lessons](../LESSONS.md).
