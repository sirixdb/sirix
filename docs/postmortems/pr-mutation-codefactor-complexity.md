# PR mutation disposition validation complexity

- Fix / issue: PR #1466, CodeFactor on `c5faca7f`.
- Symptom: CodeFactor flags `evidence_problems`, `entry_problems` and
  `judge_entry` as complex methods. The 153 focused behavior tests pass.
- Trigger: validating reviewed dispositions combines schema, individual fields
  and verdict precedence in three large functions.
- Root cause: independent validation and judgment stages share a single method
  body, making their control flow difficult to review.
- Why tests missed it: runtime tests check the disposition contract; complexity
  is the static analyzer's responsibility. No source-presence test is appropriate.

## Required behavior and derived verification (before implementation)

1. Evidence errors must retain their exact text and order: table/schema first
   (return immediately), patch, command, baseline exit, injected exit, failure,
   then source hashes. Invalid source lists or rows stop source validation;
   earlier errors remain. Cover both valid evidence and simultaneous malformed
   fields through `parse_dispositions`, plus existing counterfactual round trips.
2. Entry errors must retain their exact text and order: kind/schema first
   (return immediately), class, mutator, method, descriptor, description, indexes,
   checked commit, justification length/placeholder, then out-of-lane test and
   evidence. Both kinds use the same common validators. Cover aggregate errors
   and schema short circuits through `parse_dispositions`.
3. Verdicts retain state, action text and first-match precedence: static stale,
   contradicted/superseded, unchanged-entry re-confirmation, matched detected/
   survived/other status, then unmatched complete/sample/unmutated outcomes.
   Cover both kinds, added/reconfirmed/unchanged entries, changed/unchanged
   methods, and complete/incomplete/absent class samples using the existing lane
   and CLI fixtures. Overlapping rules must select the earlier verdict.

These boundaries serve `run` through `judge_run`, local `dispositions --check`,
and `dispositions --remap` through parsing; evidence also round-trips from the
counterfactual command. Extraction must preserve the same paths for all callers.
CONV-201/202 and the existing error protocol are the independent oracles. Wrong
behaviors include reordered or omitted diagnostics, accepting malformed evidence,
and requiring re-confirmation instead of reporting a contradicted claim.

- Regression checks: existing focused behavior tests unchanged, plus exact
  aggregate diagnostics and overlapping-verdict cases derived above. This is a
  maintainability fix, so behavioral checks should pass before and after;
  CodeFactor supplied the original static failure.
- Lesson: [separate disposition validation stages](../LESSONS.md).

## Verification results

- Before extraction: all 153 existing focused tests passed; the expanded edge
  suite (46 tests) also passed before changing production code. Existing tests
  remain unchanged. The three added tests observe aggregate diagnostics, schema/
  source short circuits and refutation precedence through parser/lane outputs.
- After extraction: all **156** focused tests passed, with **99%** branch
  coverage (the CI floor remains 85%):

  ```bash
  COVERAGE_FILE=build/reports/python-ci-complexity/.coverage python3 -B -m coverage run \
    --branch --source=.github/scripts --omit='*/tests/*' \
    -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'
  COVERAGE_FILE=build/reports/python-ci-complexity/.coverage python3 -B -m coverage report --fail-under=85
  ```

- Differential experiment against `git show HEAD:.github/scripts/pr_mutation.py`
  (the unmodified target): **4,006** parser cases and **6,912** verdict cases
  returned identical complete outputs, including error order and action text.
  Parser generation used seed 1466, six empty/malformed documents and 4,000
  combinations of missing, mistyped, empty, pattern and valid fields. Verdicts
  crossed both kinds, three base relationships, present/missing class/method,
  absent plus seven mutant statuses, detected/changed flags, method changes,
  absent/incomplete/complete class samples and present/missing named tests.
- `mutmut` is unavailable on PATH or as a Python module; no tools were installed
  or searched for elsewhere. A targeted in-memory mutation experiment ran 41
  existing/new behavior tests per mutant. All **14/14** mutants were killed:
  bypass each of the seven extracted patch/execution/hash/source/identity/
  location/justification validators; bypass re-confirmation; unconditionally
  accept a matched mutant; ignore unmatched entries; skip contradiction; skip
  supersession; reverse identity/location error order; skip touched-file hash
  coverage. No survivors require dispositions. This scoped experiment does not
  replace a full mutmut campaign or revise historical mutation totals.
- Supplementary local AST decision counts (branches, boolean alternatives,
  comprehensions and conditional expressions): evidence **26 → 3**, entry
  **25 → 8**, judgment **29 → 9**; every extracted helper is at most **10**.
  This is a local control-flow metric, not CodeFactor's analyzer or a source-text
  behavior test. The driver supplied the original CodeFactor findings; a remote
  rerun belongs to the outer executor.
- Re-traced `run`/`judge_run` and local check/remap parsing: schema errors still
  short-circuit, all later diagnostics append in original order, and static
  staleness/refutation still precede scope/re-confirmation and matched/unmatched
  decisions. Successful evidence, both valid kinds and accepted survivors take
  the same paths. Redundant matched-mutant guards and the old eager `new` local
  were removed; no unused helper or parameter remains.
- `git diff --check` passes. `release.yml` is untouched, including its separately
  reported pre-existing finding. No analyzer suppression or configuration changed.

Local generated evidence: `build/reports/python-ci-complexity/mutation.json` and
`differential.json` (ignored build output).
