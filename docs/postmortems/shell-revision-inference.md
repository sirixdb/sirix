# Bug: inferred resource access was mistaken for execution

- Fix / issue: review finding R1 for the shell presentation change.
- Symptom: the prompt displayed a historical revision after an explicit commit, or a resource
  mentioned only in a skipped conditional branch.
- Trigger: at revision 4, execute `sdb:commit(jn:doc('shop','products',1))`, or execute
  `if (false()) then jn:doc('shop','products',1) else 42`; XML had the same issue.
- Root cause: scanning arbitrary source for a literal call established neither execution nor
  the final revision. The updating-expression flag does not identify all mutating functions.
- Why tests missed it: only unconditional literal reads and the demo's updating expressions
  were exercised; neither nested mutating calls nor skipped/deferred access was checked.
- Regression test: `ShellPromptTest.executedNestedAndSkippedReferencesStayNeutralForBothStores`
  executes real JSON/XML queries against revision 4, checks neutrality after the historical
  commit and skipped/deferred calls, then verifies historical revision 1 and current revision 5.
  The original scan reports resource context for these uncertain calls, violating the oracle.
  `unsupportedAndIncompleteUpdateShapesStayNeutral` covers malformed/compound input and every
  truncated prefix; the existing demo test retains its independent 1-2-3-4-1 oracle.
- Fix: constrain inference at its shared boundary to standalone literal calls and the simple
  latest-revision JSON update forms; remove the updating flag from the prompt API and caller.
  All other expressions stay neutral without query re-evaluation or engine changes.
- Lesson: [infer context only from supported executed forms](../LESSONS.md).

## Scoped mutation review

The review run targets `ShellPrompt` and `ShellHighlighter`, limiting mutation to `reference`,
`simpleUpdate` and `tokens`; other methods are unchanged by these fixes. Main and all shell
unit tests are selected for ordinary verification, with the unrelated temporal-reader task
excluded. PIT selects the shell tests. The local init script drops comma-containing JVM
properties that PIT otherwise splits; production build configuration is unchanged.

Individual justification for `ShellPrompt.reference`, line 78,
`ConditionalsBoundaryMutator` (`tokens.size() < 14` to `<= 14`): equivalent. At exactly
14 tokens, a literal two-argument source ends at token 9, so `simpleUpdate` starts at 10
and rejects the four remaining tokens with `start + 4 >= tokens.size()`. An explicit
historical revision or a store source is rejected by the bound-source check instead.
Both versions therefore return null at the only differing boundary.

The first mutation run exposed four non-equivalent survivors in `simpleUpdate`: changing
its initial length boundary or addition, returning true at its rejection, and changing the
replacement-target length boundary. Explicit neutral-result assertions on incomplete
replacement expressions and unsupported whole-document deletion cover these cases in
`unsupportedAndIncompleteUpdateShapesStayNeutral`.

Two timeouts occur in unchanged quote scanning inside the mutated `tokens` method:

- `ShellHighlighter.tokens`, line 94, `IncrementsMutator`: reversing the increment after a
  backslash makes the next iteration revisit that same backslash forever. The escaped-quote
  inputs in `incompleteEscapedAndUnicodeInputRoundTripsWhileTyping` exercise it.
- `ShellHighlighter.tokens`, line 97, `IncrementsMutator`: reversing the increment over a
  doubled quote returns to its first quote, repeating the doubled-quote branch forever.
  The `'it''s'` input in the same test exercises it.

These timeouts are non-progress mutations, not slow successful queries. They remain explicitly
reported as timeouts rather than ordinary kills; no wall-clock assertions were added.

## Verification outcome

The completed focused run passed all 34 Main/shell tests. Its PIT XML records 175 mutants:
168 `KILLED`, two `TIMED_OUT` as justified above, five `SURVIVED`, and zero `NO_COVERAGE`.
The four non-equivalent survivors prompted the three additional neutral-result assertions
described above. The final test/PIT repeat then waited over 38 minutes for shared `heavy.sh`
capacity without starting Gradle and was cancelled. Those final test-only additions and their
expected mutation kills remain unverified; the last completed run is not claimed as their proof.
The completed report remains at `build/review-r1-r2/pitest/mutations.xml` in this worktree.
