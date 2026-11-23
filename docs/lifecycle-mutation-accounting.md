# Lifecycle mutation accounting

## Current candidate closure (round 101)

TXN-804 now has behavior coverage for null creation arguments and closed peer handles
across JSON/XML pre-hooks, post-hooks and revision listeners. The early creation guard
remains: its exact sampled index-7 mutant is **KILLED** by the live-handle/null-argument
case in `allLifecycleEntryPointsRejectCallbackReentry` (invocation 17). Omitting the
same guard separately fails all six closed-handle callback cases; exact restoration
passes all six. These error cases reject a different error class or a diagnostic that
omits the callback restriction, rather than inspecting implementation text.

The redundant `Retirement.close` wake is removed under CONV-103 resolution (4).
Retirement release alone satisfies no waiter predicate; delivery, pre-hook completion,
rejected submission and terminal close retain their predicate-change notifications.
No universal cost-equivalence disposition is asserted. Its removal changes the class's
stride sample: current indices 13 and 29 enter the sample, while the old deferred-loop
index 69 and removed wake index 83 are absent. Absence is **not KILLED**. The
[identity comparison](postmortems/evidence/lifecycle-work-budgets/round101-retirement-sample-churn.json)
preserves every old/current status, including Math index 26 and conditional index 37
changing from raw KILLED to raw TIMED_OUT. Historical assertion controls remain historical;
they are not substituted for current raw timeouts.

TXN-608 recovery cleanup now has an observable public control: back up a clean resource,
establish recovery responsibility, remove that resource through `Database.removeResource`,
then restore the clean backup at the same path and persisted database/resource identity.
Writer admission must preserve the backed-up value and must not recreate the deleted
resource's `.commit` marker. Omitting the current single-key removal fails this persisted
marker assertion for **both JSON and XML**; restoring it passes both, followed by a
successful commit/readback. This detects the cleanup effect without private-map assertions.
It is not a general heap-retention proof, a claim that the helper null-return mutants
prove removal, or a relabeling of the old predicate's SURVIVED result.

[Control patches, commands, failures and restored source hashes](postmortems/evidence/lifecycle-work-budgets/round101-contract-controls.json)
are retained with compressed JUnit XML. The complete lifecycle suite passes **359 cases**;
the unchanged work/allocation matrix passes **37**, and Python mutation tooling passes
**161**. Prior scan/allocation defect controls and their exact restorations remain as
recorded below; no work ceiling is widened. All **51** selected compiled classes still
match the prior candidate's actual-main comparison byte for byte:
[compiled continuity](postmortems/evidence/lifecycle-work-budgets/round101-compiled-continuity.json).
That is compiled evidence for those classes, not native/JIT latency equivalence.

All **six original scopes completed** at the frozen candidate, with all **3,052**
source/configuration hashes unchanged. Targets, selected tests and per-class sampling caps
match the previous original-scope run exactly. The current 45 changed-line mutants are
**41 KILLED + 4 TIMED_OUT; zero SURVIVED and zero NO_COVERAGE**. Raw timeouts count as
DETECTED under CONV-202; none is described as an assertion kill.

| Scope | KILLED | SURVIVED | NO_COVERAGE | TIMED_OUT | Changed lines |
| --- | ---: | ---: | ---: | ---: | --- |
| Core 1 | 27 | 7 | 7 | 7 | 13 K / 3 TO |
| Core 2 | 12 | 22 | 17 | 2 | 1 K / 1 TO |
| Core 3 | 6 | 13 | 16 | 0 | 2 K / 0 TO |
| Core 4 | 13 | 4 | 29 | 0 | 3 K / 0 TO |
| Query | 36 | 5 | 57 | 1 | 8 K / 0 TO |
| REST | 21 | 0 | 0 | 0 | 14 K / 0 TO |
| **Total** | **115** | **51** | **126** | **10** | **41 K / 4 TO** |

[Final accounting and original selections](postmortems/evidence/lifecycle-work-budgets/round101-sampled-accounting.json),
[raw command provenance](postmortems/evidence/lifecycle-work-budgets/round101-commands.json),
[frozen source manifest](postmortems/evidence/lifecycle-work-budgets/round101-source-manifest.json.gz)
and the six compressed XML reports retain the exact raw results. Whole-class survivors
and uncovered mutants outside changed lines remain advisory debt, not new waivers.
The unchanged `TransactionExecutionLock.assertCanClose`, index 5, remains raw TIMED_OUT;
its prior exact-control provenance is retained without relabeling the current result.

The actual mutation consumer's [local verdict is clean](postmortems/evidence/lifecycle-work-budgets/round101-local-verdict.json):
all modules satisfy the changed-line criterion, and the unchanged disposition file has
zero errors or failing entries. Its first local check lacked the compiled CLI class for
two pre-existing entries outside the mutated scopes. Compiling that unchanged class, an
ordinary CI `classes` prerequisite, resolved the [recorded evidence gap](postmortems/evidence/lifecycle-work-budgets/round101-consumer-prerequisite.json);
no disposition, source, lane, target or sampling change was made. The initial verdict is
retained separately. No full mutation rerun was required for this compiler prerequisite.
This local uncommitted-candidate verdict does not attest hosted CI or a future committed head.


The earlier snapshots below retain their historical counts and qualifications. The new
public restore control supersedes the earlier recovery-evidence gap, without changing any
historical raw status. The outer executor owns publication, attestation and committed-head
readiness. Preserve the captain-approved `new-default` classification, Vision check,
speed/specification `cannot tell` qualifications and `Closes #1379`. No timing benchmark
or timing-slot request is authorized or performed here.

## Prior candidate verification (round 100)

The repair based on b19 adds the Firstmate097 CONV-301 commit/close budgets for
JSON/XML, 16/4,096 values, all three KEEP_OPEN lifetimes, and writer/session/database
retirement with eight readers and a peer retaining the storage pool. It preserves the
original work ceilings and separates document visits from XML name-dictionary work;
final storage-pool safety flushing is outside this fixture. The existing all-index
record total is preserved. Cached cursor moves and direct storage reads each supply
positive controls on both counters.

Local evidence (candidate sources, not a new committed-head CI verdict):

| Verification | Observed result |
| --- | --- |
| Core lifecycle/regression and existing work budgets | 1,124 main cases, including 338 budget-selector cases; zero failures. Prerequisite forks: 56 cases, zero failures, two expected skips under `sharedArenaDiagnosticSkipTest` because deterministic arena closing is disabled. |
| Final lifecycle budget matrix and clean lock allocation | 37 cases passed, no skips. This final focused matrix supersedes the initial 12-case synchronous-only fixture. |
| Independent cost defect controls | An extra document scan at commit and a per-admission object allocation fail all 37 cases; restored sources pass all 37. The allocation arm observes 262,144 bytes against zero. Exact patch, commands, assertion output and restored source hashes are in [the control record](postmortems/evidence/lifecycle-work-budgets/budget-controls.json) and [injection patch](postmortems/evidence/lifecycle-work-budgets/budget-defects.patch). These are cost regressions, not claims that the original implementation failed the new budget. |
| Query budgets and ownership | 359 cases, zero failures, five opt-in 100,000-item cases skipped because `SIRIX_VALID_TIME_LARGE_BUDGET` is not enabled; five clean allocation cases passed. The separately selected traversal-ownership suite passed all six cases. |
| MCP budgets / REST offline iterator ownership | Three / ten cases passed, no skips; no Docker/Compose tasks executed. |
| Python mutation tooling | 161 cases passed. |
| Formatting and whitespace | Core Spotless applied successfully; `git diff --check` passed. |
| Actual-main compiled evidence | Fresh compilation of `58ec1a784abfe54a9130ea287ebc2631faec6b7e`; 51 selected base/candidate classes, including all emitted `ParallelJsonShredder` nested classes and worker lambda bodies in the enclosing class. [Class/source hashes](postmortems/evidence/lifecycle-work-budgets/compiled-comparison.json), [core diff](postmortems/evidence/lifecycle-work-budgets/sirix-core-compiled.diff.gz), [query diff](postmortems/evidence/lifecycle-work-budgets/sirix-query-compiled.diff.gz). Intended lifecycle changes and gated diagnostic calls differ; this is not native/JIT latency equivalence or a blanket unchanged-success-path claim. |

All JVM/Gradle verification uses the required heavy admission wrapper and 6 GB memory /
60 GB disk floors. Final validation retains separate Docker-free task graphs. REST uses `offlineTest`.
No timed benchmark was run, and no latency slot is requested while other acceptance
evidence remains unresolved. The approved new-default classification and its captain
approval, Vision check, speed/specification `cannot tell` rows and `Closes #1379` must
be preserved by the outer pipeline when publishing these notes.

Supplemental, unsampled changed-method PIT completed with **10 KILLED, zero survivors,
uncovered mutants or timeouts**: eight diagnostic mutants and two marker-path return
mutants. [Exact identities and killers](postmortems/evidence/lifecycle-work-budgets/supplement-pit.json)
and [raw XML](postmortems/evidence/lifecycle-work-budgets/supplement-mutations.xml.gz) are retained.
`forgetCommitRecovery` now has no prefix predicate; the default mutators generate no
mutant for its direct map removal. This is removal of the old mutated code, not evidence
of a new public retention oracle or a relabeled historical kill.

### Completed candidate sampled scope

All **six original scopes completed** at the candidate sources: four original core partitions,
query and REST. Every target list, selected test list and per-class sampling cap was checked
against the original CI selection; none was reduced or widened. The manifest of **3,052**
source/configuration files verified unchanged after execution. This is new candidate evidence,
separate from the b19 snapshot and the ten-mutant supplement.

| Scope | KILLED | SURVIVED | NO_COVERAGE | TIMED_OUT | Changed-line result |
| --- | ---: | ---: | ---: | ---: | --- |
| Core 1 | 27 | 9 | 7 | 5 | 13 K / 2 S / 1 TO |
| Core 2 | 12 | 22 | 17 | 2 | 1 K / 1 TO |
| Core 3 | 6 | 13 | 16 | 0 | 2 K |
| Core 4 | 13 | 4 | 29 | 0 | 3 K |
| Query | 36 | 5 | 57 | 1 | 8 K |
| REST | 21 | 0 | 0 | 0 | 14 K |
| **Total** | **115** | **53** | **126** | **8** | **45 = 41 K / 2 S / 2 TO; zero uncovered** |

[Candidate accounting, exact selections and report hashes](postmortems/evidence/lifecycle-work-budgets/candidate-sampled-accounting.json),
[execution provenance](postmortems/evidence/lifecycle-work-budgets/candidate-pit-provenance.json) and
[source manifest](postmortems/evidence/lifecycle-work-budgets/candidate-source-manifest.json.gz)
are repository artifacts; the accounting names each retained raw XML archive.
The candidate's source rewrite changes the evenly sampled identities; the changed-line
count is therefore 45 rather than b19's 46. The query return-zero mutant in
`OrderedUnionAxis.nextKey`, line 59, is raw `TIMED_OUT` in this run, outside changed lines;
its historical `KILLED` result is not substituted. These differences establish no speed gain.
The older `enqueueNotification` conditional at instruction index 5 is no longer in the
sample after the helper refactor. Its absence is sample churn, not a current kill;
the prior control retains its historical source and oracle qualifications.

The final two changed-line timeouts map to the accepted decisions 063/079 controls:
`DatabaseNotifications$Retirement.close()V`, line 328, NegateConditionals index 69
(historical line 322, deferred-delivery control), and
`TransactionExecutionLock.assertCanClose()V`, line 12, NegateConditionals index 5
(public resource-creation/bootstrap-close control). The production methods are unchanged
from b19. Retain their scoped **DETECTED** interpretation with the prior exact-control
provenance; neither is raw `KILLED`, and the private/helper-only withdrawn controls are
not reinstated.

The two survivors below retain their reviewed scoped qualifications, with unchanged
production sources and exact final identities. They remain raw `SURVIVED`; no universal
cost-equivalence claim or unsupported automated disposition entry is added. Thus the
sampled scope is complete, while automatic mutation acceptance and hosted-CI readiness
are **not claimed**. The 270-second CI scheduling issue remains the separate follow-up
required by Steering099. Outer publication and final committed-head checks remain required.

## Reconciled b19 evidence (Firstmate099/100)

The original six sampled scopes at `b19c6bc0b1eb961c990689174b150a7836e74690`
are complete: CI workflow `45060138272` supplied core partition 4, query and REST;
the authorized local completion supplied core partitions 1–3 without changing their
targets, tests or sampling caps. The verified report hashes and per-scope raw counts
are recorded in [the b19 accounting artifact](postmortems/evidence/lifecycle-work-budgets/b19-sampled-accounting.json).
Total: 302 mutants, raw **116 KILLED, 53 SURVIVED, 125 NO_COVERAGE, 8 TIMED_OUT**.
Changed-line scope: **46 = 41 KILLED + 2 SURVIVED + 3 TIMED_OUT**, zero uncovered.
The reporter's 44 detected includes raw timeouts; it does not establish assertion kills.

The two changed-line survivors have been re-mapped to the unchanged candidate sources:

| Final b19 identity | Review against decisions 063/079 |
| --- | --- |
| `DatabaseNotifications$Retirement.close()V`, line 333, VoidMethodCall, index 83, remove `Object.notifyAll` | Same method and index as historical line 327. `awaitProgress` depends on pending delivery/pre-hooks, not the retirement count or admission flag. Delivery, pre-hook completion, submission rejection and stopped close each notify when they change that predicate. Releasing retirement alone cannot satisfy it. Retains the accepted **functional** equivalence, without a claim about redundant wake cost. |
| `DatabaseHandle.createResource(ResourceConfiguration)Z`, line 44, VoidMethodCall, index 7, remove `assertNotInCallback` | Source unchanged. Valid live-resource creation still reaches callback rejection in the mutation-gate/retirement boundaries before waiting. Retains the accepted **scoped downstream redundancy**, not universal equivalence for invalid arguments or closed handles. |

No unsupported universal-equivalence entries have been added to the automated disposition
file for these scoped decisions. Raw survivors remain survivors. This repository review is
not a claim that the CI reporter accepted them or that every acceptance condition is green.
Steering100 removed the CodeFactor integration; its carried finding is no longer applicable.
The CI 270-second scheduling problem is separate follow-up: no lane, target, sampling or
execution-share configuration is changed here.

The unsampled recovery predicate's old public/private controls remain qualified below.
The candidate removes that predicate and its process-wide prefix scan in favor of direct
removal of the resource's single marker key, shared with writer admission. It does not
claim a behavioral kill of the old private-map oracle. New CONV-301 work/allocation
budgets are specified before implementation; final candidate execution and mutation
results must be recorded separately from this historical snapshot. PR publication and
exact committed-head readiness remain owned by the outer pipeline.

## Earlier snapshots and accepted qualifications

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

## Historical remaining acceptance evidence

The following is the earlier snapshot's remaining-work record, not a request to repeat
completed candidate scopes. Current evidence and remaining outer-executor work are above.

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
