# Bug: callback tests omitted argument and handle-state boundaries

- Fix / issue: PR #1353, CI repair 101; TXN-804, TXN-803/805 and TXN-608.
- Symptom: the sampled creation-guard mutant survived despite a callback matrix;
  recovery deletion/recreation did not distinguish retained recovery bookkeeping.
- Trigger: create through the callback's database handle with a null configuration or
  through a closed peer handle; restore a clean backup at the removed resource's exact
  path and persisted identity after establishing a recovery obligation.
- Root cause: valid live-handle creation reaches a downstream callback guard, and a newly
  created resource gets a different identity. Those cases hide the early guard's error
  precedence and the old recovery obligation's effect on the same restored identity.
- Why tests missed it: the original public matrices covered valid configuration and new
  resource identity only. The withdrawn private-map oracle did not establish a supported
  observable behavior.
- Regression tests: `allLifecycleEntryPointsRejectCallbackReentry` includes null creation;
  `closedHandleCreationRejectsCallbackReentry` covers JSON/XML and pre/post/listener
  phases; `restoredBackupDoesNotInheritDeletedRecovery` checks persisted marker absence,
  equal restored identity and committed values through backup/restore and writer APIs.
  Counterfactual evidence and actual outcomes are recorded in the mutation accounting.
- Correction: retain the early creation guard and the direct one-key recovery removal.
  Remove the redundant retirement-release wake: it cannot satisfy a waiter predicate;
  delivery, pre-hook completion, failed submission and terminal close retain their own
  notifications. No waiting deadline or work ceiling changes.
- Lesson: [check lifecycle error precedence and identity reuse](../LESSONS.md#check-lifecycle-error-precedence-and-identity-reuse).

The retirement trace has two waiter predicates: retirement waits for `pending` and
`preHooks`, while idle waiting additionally includes `deferred`. Releasing an empty or
nonfinal retirement changes none of them. Replaying deferred work moves it into pending;
completion signals the waiter. Rejected submission signals when it undoes pending work,
and terminal close signals when it clears deferred work. Retire-history and enclosing
session/handle/database callers retain that same protocol. No helper or caller becomes
unreachable after removing the extra wake.

Local contract control: the 359-case suite passes; omitting the early creation guard
fails six closed-handle callback cases and restoring it passes six. Omitting recovery
removal fails the persisted-marker assertion in both JSON/XML restore cases; restoring
it passes both. Patches, commands, assertion output, XML and exact source-restoration
hashes are archived with the final mutation accounting. These are public contract
controls, not a rewrite of historical PIT statuses or a general heap-retention proof.
