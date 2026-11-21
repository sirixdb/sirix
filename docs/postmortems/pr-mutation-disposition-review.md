# Bug: Mutation disposition review boundaries

- Fix / issue: review findings R1–R4 on `fm/sirix-ci-mutant-dispositions`.
- Symptom: a test constrained private helper calls; valid associations could be rejected or
  omitted; remap could hide a moved counterpart; evidence could omit a deleted source's hash.
- Trigger: two out-of-lane entries; `Main.kt` declaring `MainKt` or multiple differently named
  types in one file; a same-class remap decoy alongside a moved method; a deletion patch
  concatenated with an in-place mutant injection.
- Root cause: four boundaries used weaker substitutes for their contracts: a call count for
  the observable verdict, a filename for binary-class ownership, the first matching tier for
  all eligible proposals, and new-side paths alone for all patch effects. Declaration discovery
  now serves both validation and selection. Patch acceptance rejects non-in-place edits before
  either consumer can hash or execute anything. Remap deduplicates by identity across all tiers.
- Why tests missed it: fixtures paired filenames with class names, remap tiers were exercised
  separately, a deletion case explicitly blessed missing evidence, and the two-entry test
  asserted an unspecified implementation detail. Historical call-count-only kills are now
  classified as unresolved in the reporter's mutation record.
- Regression test: `VerdictTest.test_two_out_of_lane_entries_are_accepted_and_reported`,
  `AssociationTest.test_associations_follow_every_declared_binary_class_of_a_source`,
  `RefactorHelperTest.test_remap_keeps_all_overlapping_tiers_and_deduplicates_reports`, and
  `SecondRoundTest.test_patch_creation_deletion_and_rename_are_rejected_by_every_consumer`.
  Oracles are CONV-201/203/204, Java/Kotlin declaration ownership, normalized emitted TOML
  proposals, persisted verdicts and summary entries, and refusal before command side effects.
  They reject filename-derived ownership, hidden moved candidates, duplicate proposals,
  invented facades from constructor properties, and incomplete patch evidence.
- Verification: one focused final invocation runs `test_pr_mutation*.py`, replays the ownership,
  remap and patch regressions against the starting commit in memory, and injects targeted
  mutations into the corrected boundaries. No repository-wide test, lint, pipeline or CI phase
  is run here.
- Lesson: [check ownership and evidence at shared boundaries](../LESSONS.md).

## Focused verification evidence

The final invocation passed all **149** tests selected by
`python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
The same invocation executed the ownership, overlapping-remap and patch-refusal regressions
against the starting commit `4cc544e52600b39aac3994199632d34e79676868`, loaded in memory:
all three failed with assertion failures, then passed against the corrected code.
The expected patch-application errors printed by negative counterfactual tests did not fail
the suite.

Eleven independent, single-site in-memory mutations were each rejected by a behavior assertion
(no errors, timeouts or internal call-count assertions counted as kills):

| Mutation | Observable regression oracle |
| --- | --- |
| Omit Kotlin file facades | A valid facade association must load and select its test. |
| Invent a class from the filename | A source with differently named declared types must not validate the filename as a class. |
| Retain only one top-level declaration | Both declared types' associations must load and select both tests. |
| Select associations by filename | Every declaring source must reach its associations despite a different filename or directory. |
| Stop at the first remap tier | A moved counterpart must remain in the emitted proposals beside same-class candidates. |
| Emit duplicate remap identities | Repeated reports and overlapping tiers must produce each proposal once. |
| Drop instruction-index preference | A same-index renamed candidate must precede a different-index candidate in its tier. |
| Skip creation/deletion headers | Evidence validation and counterfactual execution must refuse incomplete coverage. |
| Allow different old/new patch paths | Rename evidence must be refused before running the test command. |
| Accept only the first disposition | Both mutants must be accepted, with a successful lane result. |
| Emit only the first summary entry | Both accepted mutants must appear under the out-of-lane summary heading. |

No targeted mutant survived. This focused result does not reclassify unknown kills from the
historical aggregate: those relying only on the withdrawn spy assertion remain unresolved.

## Follow-up: R5–R7 shared-boundary coverage

- Fix / issue: review findings R5–R7 against `08e52391c13277c167e00b2086fc6934b389dc96`.
- Symptom: the select test still required private call arguments; a Kotlin class literal could
  hide the next main or test declaration; a header-only Git rename could escape evidence hashes.
- Trigger: a base branch and PR with different disposition contents after their common ancestor;
  `val token = String::class` followed by `class Foo`; an ordinary injection combined with a
  pure rename, copy, file creation/deletion, binary patch or mode-only section.
- Root cause: the prior review did not trace the test-oracle and patch-effect invariants across
  every representation. The shared scanner consumed `class` after a member selector as a type
  declaration. The patch boundary looked only at paired text headers, while Git also changes
  files through metadata. Non-declaration references are removed before shared declaration
  scanning; unsupported patch operations are rejected at the boundary used by both consumers.
- Why tests missed it: private mocks supplied the expected base content; scanner cases lacked
  literals immediately before declarations; patch cases all had `---`/`+++` headers.
- Regression tests: `CommandLineTest.test_select_cli_writes_manifest_from_divergent_merge_base`
  observes the ancestor's content in `selection.json` through the real `select` command.
  `AssociationTest.test_kotlin_class_literals_preserve_association_ownership_and_test_discovery`
  exercises association validation, changed-source selection and test discovery with class
  literals, spaced selectors, JVM class references and object expressions before class/object
  declarations. `VerdictTest.test_class_literal_does_not_make_an_existing_out_of_lane_test_stale`
  observes an accepted verdict for the declared Kotlin test. Both new patch tests in
  `SecondRoundTest` reject metadata effects before command side effects, and verify ordinary
  Git text injection still returns complete hashes and restores the original bytes.
- Evidence policy: historical kills supported only by the removed private spy assertions remain
  unresolved in `MUTATION_DISPOSITIONS.md`; no internal assertion is used as fresh kill evidence.
- Lesson: trace a parser's non-text representations and every consumer of its identities;
  distinguish input provenance using real persisted outputs rather than private helper calls.

### Follow-up verification evidence

One final focused invocation passed all **153** `test_pr_mutation*.py` tests. Loading the
starting commit `08e52391c13277c167e00b2086fc6934b389dc96` in memory reproduced assertion
failures in the association/discovery, named-test staleness and header-only patch regressions.
The rewritten manifest test passes the already-correct implementation; its improvement is an
observable oracle, demonstrated by the two provenance mutations below.

All **nine** targeted in-memory mutations were killed by behavioral assertions, with no
unexpected test errors used as kill evidence:

| Mutation | Observable oracle |
| --- | --- |
| Remove the class-literal filter | Declared main/test names remain valid and selected. |
| Ignore whitespace in member selectors | Spaced Kotlin references preserve subsequent declarations. |
| Remove Git metadata refusal | Unsupported operations fail validation and never execute the test command. |
| Allow file-mode metadata | Empty-file creation/deletion and mode-only changes are rejected. |
| Allow rename/copy metadata | Pure rename/copy sections cannot escape source hashes. |
| Allow binary metadata | Both Git binary formats are rejected. |
| Allow dissimilarity metadata | Unsupported rewrite metadata is rejected at the same boundary. |
| Read the base tip's dispositions | The manifest must contain the common ancestor's distinct content. |
| Read the PR head's dispositions | The manifest must contain the common ancestor's distinct content. |

No targeted mutant survived. Ordinary Git text injection also passed its baseline/injected
exit-status, complete SHA-256 evidence and byte-identical restoration control. Earlier R2–R4
regressions remained green. No repository-wide validation or other pipeline phase was run.
