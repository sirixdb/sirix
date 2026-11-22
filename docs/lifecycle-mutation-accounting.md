# Lifecycle mutation accounting

This restores the qualified dispositions omitted from PR #1353's rewritten description.
It records historical evidence and accepted decisions, **not final-head acceptance**.
The CI repair started at `edd24138101c0b7fb636430bc32a36cb88fd2269`, based on
`58ec1a784abfe54a9130ea287ebc2631faec6b7e`. Fresh full-scope PIT and a compiled
main/head comparison, including `ParallelJsonShredder` and its nested workers, remain
outstanding. Source-only cleanup does not supply that evidence.

The subsequent CI repair based on `316d75f6ddaf5a7bebe1c2ab7d8709c3a4fdc337`
corrects database close retry/admission and the Python mutation-selector failures.
Its focused, unsampled run over `LocalDatabase.close` and `closeQuiescent` reports
14 `KILLED`, five `SURVIVED`, three `NO_COVERAGE`, and no timeouts. The three mutants
on **that repair's** changed lines are all killed; the other eight records are outside
that repair's diff. This does not judge all of PR #1353's changed lines or supply the
missing four-partition/query/REST proof, compiled comparison, or a new disposition.
See [the CI close-retry write-up](postmortems/lifecycle-ci-close-retry.md).

The governing evidence rule is [CONV-102](../spec/CONV-conventions.md#conv-102--conformance-and-evidence-honesty).
Behavioral oracles come from TXN-503 (cleanup/error preservation), TXN-504 (worker
lifetime) and TXN-804 (callback rejection), rather than private maps or permit counts.

## Accepted historical dispositions

The identifiers below retain the **historical** method, source line, mutator and index.
They must be mapped to the final head before judging its changed-line report.
Decisions 063 and 079 qualify the dispositions as follows:

| Historical mutant | Raw status | Accepted interpretation and evidence |
| --- | --- | --- |
| `DatabaseNotifications$Retirement.close`, line 317, Math, index 26 | `TIMED_OUT` | `DETECTED` for this PR: the exact subtraction-to-addition control fails the assertion that the last retirement reopens admission. |
| `DatabaseNotifications$Retirement.close`, line 318, NegateConditionals, index 37 | `TIMED_OUT` | `DETECTED` for this PR: the exact control fails the assertion that an overlapping retirement continues to retain admission. |
| `DatabaseNotifications$Retirement.close`, line 322, NegateConditionals, index 69 | `TIMED_OUT` | `DETECTED` for this PR: the exact control fails the deferred-delivery assertion. |
| `TransactionExecutionLock.assertCanClose`, line 12, NegateConditionals, index 5 | `TIMED_OUT` | `DETECTED` for this PR: replacement public resource-creation/transaction-close controls produce two assertion failures. The old helper-only oracle is withdrawn. |
| `DatabaseNotifications$Retirement.close`, line 327, remove `notifyAll`, index 83 | `SURVIVED` | Accepted functional equivalence: closing retirement cannot satisfy `awaitProgress` without another completion/rejection notification. This argument is scoped to this notification protocol. |
| `DatabaseHandle.createResource`, line 44, remove callback guard, index 7 | `SURVIVED` | Accepted downstream redundancy for valid live resources: both public post-hook controls still reject before locks through another guard. This is not universal equivalence. |
| `AbstractResourceSession.lambda$forgetCommitRecovery$0`, line 219, BooleanFalseReturnVals, index 6 | `SURVIVED` in the public exact control | **Unresolved behaviorally.** Public deletion/recreation and writer commit do not detect the mutant. The historical kill used a removed private-map assertion and is withdrawn. No functional-equivalence or no-retention claim is accepted. |

`DETECTED` is a scoped reviewed disposition, **not** raw `KILLED`, a rewritten PIT
status or a budget exception. The recovery survivor remains an acceptance gap.

Provenance: the operator's archived `spec-oracles-079/pr-notes-prepared.md`,
`pr-body-prepared.md` and `publication-check-081.json` record the previously published
notes and readback. Exact controls are archived under `counterfactuals-054/retirement-count`,
`retirement-condition`, `retirement-delivery`, `spec-oracles-079/close-control`,
`create-control`, and `recovery-release-control`; the notification equivalence argument is
in `local-oracles-051/mutation-oracle-map.json`. These are external evidence identifiers,
not repository artifact links or newly executed results.

## Remaining acceptance evidence

Run all four original core CI PIT partitions and the affected query/REST scope at the
repaired final head, retaining the original selection and sampling caps. Re-map moved
methods and changed lines, preserve raw statuses, and justify each surviving/uncovered
changed-line mutant independently. Revalidate the twelve historical touched-test killer
records and affected exact controls; historical counts do not establish current kills.

The historical PIT059 report contained 190 mutants: 61 `KILLED`, 49 `SURVIVED`,
72 `NO_COVERAGE`, 8 `TIMED_OUT`; its changed scope contained 18 `KILLED`, 2 `SURVIVED`,
4 `TIMED_OUT`. Those numbers do not validate the integrated head. The former two RAM
readback failures are not an accepted current blocker after the mainline RAM fix;
the required committed-content readback expectations remain unchanged.

Publish these qualifications in the PR while preserving the existing attestation and
`Closes #1379`, then verify public readback through `gh-axi`. Preparation of this
repository note does not establish publication or CI readiness.
