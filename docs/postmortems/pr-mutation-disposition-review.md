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
