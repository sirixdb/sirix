# Bug: random salts in byte-pinned fixtures and unbounded PIT baseline

- Fix / issue: PR #1354, CI run 43720768052.
- Symptom: Linux, macOS and Windows report four failed byte comparisons; changed-source PIT exhausts its 1320-second deadline without a report.
- Trigger: enable record-page salts by default, then run golden-page, derived-elision and string-region fixtures, or select mutation tests for the four changed production sources.
- Root cause: the fixtures inherited the production randomness default, although their oracle is a fixed encoding or equality of independently created frames. PIT's directly named test selection could not find tests for ResourceConfiguration, PageKind or PageLayout and fell back to the entire core suite, whose baseline alone exceeds the mutation budget.
- Why tests missed it: focused salt tests covered deterministic mode but omitted older composite snapshots; the salt suites' names did not match the documented mutation selector contract.
- Regression tests: retain the existing golden constants and every frame-comparison assertion. Move existing salt assertions into directly named ResourceConfigurationTest, PageKindTest and PageLayoutTest suites; retain RecordPageSaltSourceTest. The existing selector must return these four suites instead of `*`, with the same exact production-class targets and deadline.
- Lesson: select deterministic mode at byte-fixture construction boundaries and align focused tests with the mutation consumer's naming contract. See [Lessons](../LESSONS.md).

## Behaviour and verification specification

The golden composite record page and both section fixtures must explicitly disable salts. Both path-summary variants, every derived-elision fixture, tuple and derived encoders, and their eager round trips must use the same deterministic setting. HOT pages have no salt field and retain their existing fixture. Production defaults, arbitrary-salt reader acceptance, copy/reload/reset behaviour and speculative disposable-frame ownership stay covered by the existing salt tests.

The independent oracles are the pre-change golden bytes/digest, tuple-versus-derived reconstructed frames, the published NIST vector and existing settings round trips. Random fixture salts must fail the byte assertions; changing bytes outside the salt, treating salt as a reader discriminator, reusing a removed salt, or accepting malformed seed/settings material must still fail their respective tests. No assertion is relaxed and no source-text assertion is introduced.

Before verification, trace each fixture's configuration through page construction and serialization, and each moved test through its unchanged executable interface. Confirm the real mutation selector finds every changed source's suite and that PIT produces XML within its existing budget. Record before/after and mutation evidence below.

## Results

- Rebased onto `21cef670d56a6ef25bef835d5fef16bc8b0c8f6d`. The only conflict was in `docs/LESSONS.md`; both branches' entries were retained.
- Local reproduction: 21 fixture tests, exactly the four CI failures. Evidence: `build/ci-salt/fixtures-before-retry.log`.
- Final focused run: 81 tests passed without skips, including all three affected fixture suites, golden-format and chunked-format tests, the four mutation-selected suites, salt allocation and async snapshot encoding. Supporting isolated startup/cache/storage-epoch tasks also passed, with two expected diagnostic skips. Evidence: `build/ci-salt/fixtures-final.log` and `build/ci-salt/test-results.json`.
- The golden fixture uses the default resource settings established by `XmlTestHelper.createTestDocument`, with only salt disabled. Both derived fixture path-summary variants flow through their shared deterministic configuration helper; the string-region fixture does likewise. No golden constant, byte comparison or allocation bound changed. The moved test methods retain their assertions, and the old classes were removed.
- The real changed-source selector still targets the same four production sources and all their emitted classes. It now selects `ResourceConfigurationTest*`, `PageKindTest*`, `PageLayoutTest*` and `RecordPageSaltSourceTest*`. PIT completed in **3m37s**, with a two-second coverage baseline and no mutation timeouts. All **17 generated mutations on added/changed lines were killed**. Evidence: `build/ci-salt/local-mutation/sirix-core/report/mutations.xml` and `build/ci-salt/mutation-provenance.json`.
- The full-source report also contains 348 surviving and 2,828 uncovered mutations on unchanged statements from the requested base commit. These remain coverage debt outside this salt change; the existing `--blocking` reporter consequently remains red. Each undetected mutant is recorded with its original base line and disposition in `build/ci-salt/mutant-justifications.tsv`; none is in the changed salt logic. No target, deadline, threshold, exclusion or reporter policy was weakened.
- `:sirix-core:spotlessJavaCheck` and `git diff --check` passed. This verification ran on Linux; macOS and Windows execution remains with the outer CI executor.
