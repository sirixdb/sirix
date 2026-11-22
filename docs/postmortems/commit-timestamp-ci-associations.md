# Bug: Valid mutation associations rejected by a closed inventory assertion

- Fix / issue: PR #1475, failing Python mutation tooling check on `4045111873c`.
- Symptom: the reporter accepts the repository's valid association file, but its repository
  regression rejects the additional timestamp-output and RAM-storage associations.
- Trigger: run `test_pr_mutation*.py` after adding valid CONV-204 associations alongside the
  four historical release-contract pairs.
- Root cause: the regression asserted equality with the entire historical inventory. CONV-204
  permits additional valid mappings; preserving the former pairs is a per-pair invariant,
  not a restriction on the file's complete contents.
- Why tests missed it: the repository test passed while its inventory contained only those
  four pairs. Fixture tests already exercised extensible loading and selection.
- Regression test: `AssociationTest.test_repository_files_are_valid_and_keep_the_former_contract_pairs`
  failed before the correction with the actual expanded repository file. It now uses the real
  consumer's normalized mappings, validates the complete file, and checks each required pair
  individually. CONV-204 and the historical release contract are the independent oracles;
  invalid associations and missing or changed historical pairs must still fail.
- Scope and trace: parsing and `load_associations` already accept multiple valid entries;
  `test_scope` and compiler-entry selection tests already preserve mappings, heuristic matches,
  missing-test refusal and shared caps. Only the closed inventory assertion needed correction.
  No production logic, association data or mutation acceptance rule changes.
- Verification: before the correction, 156 focused tests ran with this sole failure. After
  correction, run the same CI coverage command and its unchanged 85% floor (results below).
- Lesson: [preserve required mappings without closing an extensible inventory](../LESSONS.md).

## Local verification

```bash
COVERAGE_FILE=build/reports/ci-associations/.coverage python3 -B -m coverage run \
  --branch --source=.github/scripts --omit='*/tests/*' \
  -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'
COVERAGE_FILE=build/reports/ci-associations/.coverage python3 -B -m coverage xml \
  -o build/reports/ci-associations/python-coverage.xml
COVERAGE_FILE=build/reports/ci-associations/.coverage python3 -B -m coverage report --fail-under=85
```

All **156 tests passed**, the unchanged coverage floor passed with **99%** branch coverage,
and coverage XML generation succeeded. `git diff --check` passed. The expected patch-application
errors from negative counterfactual fixtures did not fail the suite. Only a test assertion and
its documentation changed; production mutation tooling is unchanged.

CodeFactor's separate failure reports only `3 issues found.` in GitHub's commit status.
Its report URL returns HTTP 403 via direct HTTP and the browser, and GitHub supplies no
CodeFactor annotations or review comments. No speculative analyzer fix is included.
