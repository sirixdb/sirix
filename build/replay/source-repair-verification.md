# JSON source hash/count repair

The committed source graph must be trustworthy before replay can use it as an oracle.
A skipped-root forest previously repaired only its selected last root; POSTORDER did
not maintain descendant counts, and removal hashed before unlinking. Local hashes also
include structural links, so changed boundary records need their own hash repaired.

JsonHashingMutation captures the old contributions of affected records and ancestors,
then propagates final contributions in final-tree postorder. New forest roots contribute
once each. ROLLING reads the changed boundary instead of scanning an unchanged child
prefix. Moves capture changed descendant path classes, and renames preserve the complete
subtree contribution. Auto-commit boundaries use incremental maintenance, including when
the former deferred-repair option is set. A failed bulk mutation requires rollback;
precondition failures before bulk mutation still leave the transaction usable.

All three JSON shredders now keep skipped left-sibling forests in input order. Two old
expected JSON values encoded the reversed-order defect and were corrected; work-budget
bounds were not changed.

## Verification

All Gradle JVMs used the required memory/flock limiter, taskset -c 0-11, and the private
Maven repository build/replay/m2, created empty before the first build. The Gradle home
is build/replay/gradle-home; the shared dependency cache was read-only. Test forks used
2 GiB. No changes were made to ~/.m2.

- Source-only oracle: R16, forests at all four positions, every committed intermediate
  auto-commit revision, shared-container rename and failed partial input, across all four
  versioning types, all three hash modes and both Dewey settings. Counts and canonical
  hashes are checked directly, including cold reopen; NONE retains its existing contract
  of not maintaining aggregate descendant counts/hashes.
- Hashing work budget: 18 record reads, one old boundary identity and three new identities
  written, for both 16- and 4096-element prefixes. A prefix-scan mutant and a last-root-only
  mutant failed their respective assertions; both were removed.
- First full core run: 13,161 tests, 5 failures, 0 errors, 77 skipped. Three failures were
  an unnecessary boundary read with hashing/diffs disabled; two were reversed-order
  expectations. All five were corrected. Raw log: source-core.log; XML: source-core-results/.
- Direct follow-up reproducer: 48 tests, 32 failures before their fixes (eight ROLLING
  container-rename hash failures and 24 partial-input commits that should require rollback).
  Evidence: source-followup-red.log and source-followup-red-results/.
- Final focused run: 267 tests, zero failures/errors/skips. This includes all source oracle
  cases, JsonHashingWorkBudgetTest, JsonBulkInsertCollectionWorkBudgetTest,
  JsonBulkInsertDiffRegressionTest and BasicJsonDiffWorkBudgetTest. Formatting applied
  successfully; git diff --check passed. Evidence: source-followup-green.log and
  source-followup-green-results/.

The full suite has not yet been rerun after the follow-up fixes. Full core/query and
latency acceptance remain required for the completed replay branch; these results do
not claim that the replay implementation or its performance gates are complete.
