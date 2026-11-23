# Bug: Fragment guard and round-trip boundary gaps

- Fix / issue: Changelog-fragment review findings R1, R2 and R3 on
  `fm/sirix-changelog-fragments`.
- Symptom: The lessons round-trip test moved a standalone row into the first table and failed;
  the shared-file guard accepted new level-two lesson headings; the lesson example produced a
  broken evidence link after collection.
- Trigger: Round-trip the existing lessons file, append `## New lesson` and prose directly to
  `docs/LESSONS.md`, or collect a lesson copied from the documented example.
- Root cause: The test selected a row from the whole document instead of the destination table.
  The lessons classifier omitted a heading level already supported by the changelog classifier.
  The example resolved links from the fragment directory although collection preserves the body
  at a different destination.
- Why tests missed it: Row selection lacked a destination boundary, heading cases did not cover
  both shared files at both levels, and the documentation example used the wrong link context.
- Regression test: `RenderLessons.test_the_last_table_row_round_trips_byte_for_byte` owns the
  collected-file byte contract and separately retains later row positions.
  `Guard.test_added_shared_file_headings_are_violations` uses the documented entry grammar;
  `Guard.test_rewording_shared_file_headings_is_allowed` preserves ordinary edits. Run with
  `python3 -B -m unittest discover -s .github/scripts/tests -p 'test_changelog_fragments.py'`.
  The original round-trip test fails on the existing file; reverting the lessons classifier
  makes the level-two addition regression fail. Verification evidence belongs to the review run.
- Lesson: [Collected lessons](../LESSONS.md): use the collected
  destination to define boundaries, entry grammar and link context.

## Review follow-up: Collection exemptions and permanent links

- Fix / issue: Review findings R7 and R8.
- Symptom: Deleting fragment documentation exempted direct entries in both shared files; the
  postmortem's lesson link would break when collection deleted the fragment.
- Trigger: Delete `changelog.d/README.md` or collect only a changelog fragment while adding a
  direct lesson, or follow the postmortem's lesson link after release collection.
- Root cause: A single deletion flag discarded the deleted file's name and target ownership.
  A permanent document linked to a temporary fragment instead of its collected destination.
- Why tests missed it: The collection test covered a legitimate deletion without a direct entry
  in the other target; link lifetime was not considered across collection.
- Regression test: `GuardOverGit.test_non_fragment_deletions_exempt_neither_target` and
  `GuardOverGit.test_collecting_one_target_does_not_exempt_the_other` exercise committed diffs
  through `check`; `test_the_release_collection_commit_is_exempt` retains successful collection
  for each target separately and together. Run with the focused command above.
- Lesson: [Collected lessons](../LESSONS.md): retain target ownership when applying exemptions
  and link permanent documents to permanent destinations.
