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

## FSST epoch byte-oracle follow-up

- Symptom: `StorageEpochFsstInlineDecisionTest` compares independently created stores' complete frames, so default-on random salts make the non-import property trials fail even when values, carriers and table bindings agree.
- Trigger: either fixed seed under `NONE` or `ROLLING`, with bounded-log rotation alone or explicit spilling flushes, in the same transaction or a reopened transaction.
- Root cause: the new fixture inherited production randomness rather than the deterministic configuration already used by `StorageEpochStoreIdentityTest`; the frame reader correctly retains every salt byte.
- Why tests missed it: the reported pre-rebase results did not establish that the property still passed with default-on salting.
- Behaviour and invariant: both arms must disable salts whenever complete frames are compared; import-mode trials and the separate value/decision/binding regressions must retain salting. The shared resource factory must apply the chosen policy before any page is created.
- Regression oracle: retain `plainAndRotatingTransactionsMakeIdenticalInlineDecisions` unchanged, including exact frame equality. This independent plain-versus-rotating oracle rejects salt differences and any other frame changes; the remaining tests exercise default-salted resources. Focused command: `./gradlew --no-daemon :sirix-core:storageEpochStoreIdentityTest --tests 'io.sirix.access.trx.page.StorageEpochFsstInlineDecisionTest'`.
- Mutation scope: no production logic changes; restoring salted configuration for either compared arm makes the existing exact-byte assertion reject its independent salts.
- Focused result: all 19 tests passed with no failures, errors or skips through the isolated identity task, using the supplied heavy-JVM wrapper and 1–4 GiB test heap. Report: `bundles/sirix-core/build/test-results/storageEpochStoreIdentityTest/TEST-io.sirix.access.trx.page.StorageEpochFsstInlineDecisionTest.xml`. The pre-fix failure was established by configuration and frame-path inspection in this review; no pre-fix execution or PIT run was performed.
- Lesson: the deterministic policy belongs at each byte-oracle fixture boundary, including fixtures introduced after the production default changes. See [Lessons](../LESSONS.md).

Before verification, trace each fixture's configuration through page construction and serialization, and each moved test through its unchanged executable interface. Confirm the real mutation selector finds every changed source's suite and that PIT produces XML within its existing budget. Record before/after and mutation evidence below.

## Results

- Rebased onto `21cef670d56a6ef25bef835d5fef16bc8b0c8f6d`. The only conflict was in `docs/LESSONS.md`; both branches' entries were retained.
- Local reproduction: 21 fixture tests, exactly the four CI failures. Evidence: `build/ci-salt/fixtures-before-retry.log`.
- Final focused run: 81 tests passed without skips, including all three affected fixture suites, golden-format and chunked-format tests, the four mutation-selected suites, salt allocation and async snapshot encoding. Supporting isolated startup/cache/storage-epoch tasks also passed, with two expected diagnostic skips. Evidence: `build/ci-salt/fixtures-final.log` and `build/ci-salt/test-results.json`.
- The golden fixture uses the default resource settings established by `XmlTestHelper.createTestDocument`, with only salt disabled. Both derived fixture path-summary variants flow through their shared deterministic configuration helper; the string-region fixture does likewise. No golden constant, byte comparison or allocation bound changed. The moved test methods retain their assertions, and the old classes were removed.
- The real changed-source selector still targets the same four production sources and all their emitted classes. It now selects `ResourceConfigurationTest*`, `PageKindTest*`, `PageLayoutTest*` and `RecordPageSaltSourceTest*`. PIT completed in **3m37s**, with a two-second coverage baseline and no mutation timeouts. All **17 generated mutations on added/changed lines were killed**. Evidence: `build/ci-salt/local-mutation/sirix-core/report/mutations.xml` and `build/ci-salt/mutation-provenance.json`.
- The full-source report also contains 348 surviving and 2,828 uncovered mutations on unchanged statements from the requested base commit. These remain coverage debt outside this salt change; the existing `--blocking` reporter consequently remains red. Each undetected mutant is recorded with its original base line and disposition in `build/ci-salt/mutant-justifications.tsv`; none is in the changed salt logic. No target, deadline, threshold, exclusion or reporter policy was weakened.
- `:sirix-core:spotlessJavaCheck` and `git diff --check` passed. This verification ran on Linux; macOS and Windows execution remains with the outer CI executor.
