# Bug: Fragment CLI arguments escaped repository and revision boundaries

- Fix / issue: PR #1486, SonarCloud findings `pythonsecurity:S8707` and
  `pythonsecurity:S8705`.
- Symptom: CLI input could redirect collection writes and deletions to another repository,
  and reached Git arguments without a revision-only contract. Both Sonar checks failed the
  new-code security gate.
- Trigger: Pass `collect --repo-root <another-repository>`, or pass
  `check --base-sha=--output=<path>`. The latter becomes a Git output option with `...HEAD`
  appended to its destination instead of a commit comparison.
- Root cause: An unnecessary repository selector crossed both the file-operation and Git
  boundaries. The remaining revision argument was interpolated into a Git argument without
  requiring the full commit hash supplied by CI.
- Why tests missed it: Fixtures depended on the root override, and Git tests supplied only
  valid hashes. No test checked rejection of root overrides or Git options.
- Regression test: `Collect.test_collect_rejects_repository_root_override_without_changing_files`,
  `GuardOverGit.test_check_rejects_repository_root_override`, and
  `GuardOverGit.test_check_rejects_non_hash_bases_without_creating_git_output` exercise `main`,
  real fixture files and Git. The independent oracle is the CLI contract in
  [the fragment convention](../../changelog.d/README.md#the-guard): script-owned repository,
  full lowercase commit hash, rejected arguments leave files untouched. The regressions failed
  before the fix (accepted overrides and options), then passed after it. Run:
  `python3 -B -m unittest discover -s .github/scripts/tests -p 'test_changelog_fragments.py'`.
- Lesson: [Collected lessons](../LESSONS.md): remove unnecessary destination selectors and
  validate revision inputs before passing them to Git.

The fix removes `--repo-root` for both commands and validates the base hash before either diff
call. Tests model the installed script location rather than retaining a production CLI option
solely for fixture selection. Ordinary collection, direct-entry rejection and each target's
release exemption retain their existing behavior.

Verification: all 226 repository Python tests pass with 99% branch coverage. Nine targeted
counterfactual mutations were detected: accepting invalid bases, rejecting valid bases, accepting
hash prefixes, requiring 39 or 41 digits, accepting uppercase, ignoring the validation error,
selecting the wrong script parent, and restoring the repository override. No mutation survived.

Vision check: this confines repository tooling without changing storage, query results, cost,
or engine defaults.
