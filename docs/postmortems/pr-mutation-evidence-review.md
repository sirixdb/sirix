# Bug: mutation evidence was promoted beyond what it proved

- Fix / issue: review findings R1–R6 on `fm/sirix-pit-lane-time-shares`.
- Symptom: malformed result XML could produce a clean verdict; a torn UTF-8 tail discarded
  completed evidence; scheduled class debt omitted recently changed lines. Verification records
  also called behavior-changing mutants equivalent, tested source shapes, and overstated cache reuse.
- Trigger: a zero-exit PIT run writes malformed XML plus a valid off-diff census; a deadline cuts
  the final stream/census record inside a multibyte character; or a class's only survivor is on a
  line changed within the scheduled window.
- Root cause: R1 inferred emptiness from the absence of a total after both empty reports and read
  failures. The completion transition now requires independently verified empty evidence, a zero
  exit and a non-empty fully readable census. R4 decoded the entire file before recovering records;
  decoding now happens per record, preserving the intact prefix and reporting census failures.
  R6 reused the PR's unchanged-line aggregate for a report whose scope includes every line; the
  scheduled view now consumes an all-lines class aggregate while PR attribution stays separate.
- Verification root causes: R2 judged strings by decoration rather than their consumers, accepting
  corrupted selectors, artifact names, report paths, completion states and dates. Nine claims are
  withdrawn for contract tests; four unused census-value fields' mutations disappear with those
  redundant fields. R3 demanded explicit Python syntax where omission preserves default behavior;
  literal arguments, captured output and restored files now supply behavioral evidence instead.
  R5 assumed cached task history implied matching compile inputs, although Error Prone differs
  between producer and consumers. All six task-history cache additions and the unsupported
  recompilation claim were correctly withdrawn at that stage. R14 now implements reuse narrowly:
  the mutation consumer matches Build's compiler inputs and restores a separate task-history
  archive; other consumers retain their existing restores. One invocation per module and daemon
  reuse remain in place, with the effect to be read from this PR's CI task outcomes.
- Why tests missed it: XML read failures were never combined with an explanatory census; truncated
  fixtures were ASCII; weekly survivors lived only off the diff; selector fixtures had one target;
  cutoff tests did not assert the external date representation. The AST test could reject a correct
  refactor but could not prove shell safety. No matching-input evidence supported the cache claim.
- Regression tests: `EmptyScopeTest.test_report_failures_cannot_be_resolved_by_an_off_diff_census`,
  `PartialResultTest.test_a_torn_utf8_stream_tail_preserves_finished_statuses`,
  `PartialResultTest.test_a_torn_utf8_census_tail_preserves_identities_and_reports_the_failure`,
  and `FullScopeTest.test_changed_line_survivors_are_in_the_scheduled_class_debt` reproduce
  R1/R4/R6. Empty-report forms, unreadable census evidence, exact selector bytes/report location,
  complete report labels, Git's UTC cutoff and literal subprocess arguments have companion tests.
  Focused command: `python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
  The fix-round verification also runs the four bug regressions against the starting reporter
  loaded from Git before running the focused suite against the fixed reporter; no PIT or mutmut
  run is authorized in this phase. All four regression cases rejected the starting reporter;
  all 235 focused tests passed after the fixes. The initial verification exposed a missing
  source in the scheduled-report fixture (corrected) and Git fixture interference from nesting
  temporary directories inside the worktree (rerun with normal toolchain temporary storage).
- Lesson: [Separate evidence, protocol and presentation](../LESSONS.md#mutation-evidence-contracts).
- Vision check: trustworthy mutation evidence protects the engine's bytes, results and cost;
  these fixes introduce no product default or new mutation policy.


## Follow-up: branch history, helper evidence and executable CI coverage

- Fix / issue: review findings R7–R10.
- Symptom and trigger: a topic committed before the cutoff but merged during the reporting window
  could become the weekly base and hide its merged sources. Local checks ignored census failures
  and omitted census-only classes. The new plugin tests were compiled but never executed by CI.
- Root cause: R7 treated traversal of every Git parent as the default branch's own history;
  `window_base` now selects only first-parent commits. R8 retained census identities but discarded
  their validation and class scope at the checker boundary; the checker now shares census output
  with the lane, fails invalid evidence and withholds completeness when any census is invalid.
  R9 relied on compilation and tool-classpath dependencies to run tests; the existing query/Kotlin
  job now explicitly executes the plugin test task with a small unit-test heap. R10 maintained a
  redundant unrestricted-mode override; map omission is now the sole unrestricted configuration,
  consistently reflected in action output, docs and the specification.
- Why tests missed it: window fixtures were linear; helper fixtures supplied only valid census
  evidence for classes also present in XML; no CI job owned execution of the plugin's test task.
- Regression tests: `FullScopeTest.test_the_window_excludes_topic_parents_from_its_base` uses a
  real merge history and checks that the topic's sources remain selected. `RefactorHelperTest`
  checks malformed, unreadable, torn UTF-8 and changed-line-violating census files through the CLI,
  both with and without entries, and checks census-only class staleness and sampled incompleteness.
  Focused verification covers the Python mutation-reporter package, the workflow's parsed task
  invocation and `:sirix-pit-changed-lines:test`; no full repository suite or mutation run is used.
  All three new regression cases rejected the starting reporter; all 238 Python tests and all
  34 plugin tests passed. Real Gradle configuration checks confirmed a supplied map enables
  `CHANGEDLINES` and an omitted map leaves it absent. The parsed workflow confirmed the plugin
  test task executes unconditionally in the existing query/Kotlin job.
- Lesson: preserve the provenance and validity of evidence through every consumer, and give each
  new test task an explicit CI execution owner. See [the evidence lesson](../LESSONS.md#mutation-evidence-contracts).
- Vision check: the fixes preserve trustworthy verification of the engine's results and cost;
  there is no product behavior or default change.


## Follow-up: preserved status provenance must reach disposition judgment

- Fix / issue: review finding R11.
- Symptom: an added equivalent or killed-out-of-lane entry matching a streamed changed-line
  survivor was labelled accepted and counted as dispositioned after its module timed out.
  The module still failed overall, but both `results.json` and the summary overstated its evidence.
- Trigger: PIT records `SURVIVED` on a changed line and the module fails or reaches its deadline;
  an added entry, including a re-confirmation, matches that preserved identity. Valid XML written
  before a failed or timed-out process ended exposed the same acceptance path.
- Root cause: status records retained identity and detection but lost whether their run finished.
  Class completeness protected stale-entry checks and module state protected the overall verdict,
  while matched-entry acceptance and per-mutant accounting lacked that provenance. Status records
  now carry run completion from stream recovery or XML attachment through judgment. An unfinished
  survivor is not judged, and accounting refuses to count it under either disposition kind.
  Detected statuses still reach contradiction, supersession and detected-other rules.
- Why tests missed it: partial-result tests asserted preserved counts, incomplete module state and
  a KILLED contradiction; the existing failed-run survivor test checked only the overall failure,
  leaving accepted labels and dispositioned counts unobserved.
- Regression tests: `PartialResultTest.test_preserved_survivors_never_authorize_either_disposition_kind`
  exercises stream/XML, deadline/nonzero exit, both kinds and new/re-confirmed entries through
  `run`, asserting labels, counts and summary output. Companion tests preserve completed-report
  acceptance and all detected-status rule 4 outcomes. Focused command:
  `python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
  The survivor regression rejected the starting reporter in all 16 unfinished-run cases;
  all 241 focused Python tests passed with the fix, including completed-report acceptance
  and preserved-status contradiction, supersession and detected-other outcomes.
- Lesson: [preserve evidence provenance at every consumer](../LESSONS.md#mutation-evidence-contracts),
  including positive acceptance and accounting rather than only the overall gate verdict.
- Vision check: the change prevents unsupported verification claims about the engine's results
  and cost, with no product behavior or default change.


## Follow-up: local evidence loading must not assume completion

- Fix / issue: review finding R12.
- Symptom: the lane reported a failed run's added survivor entry as not judged, while checking
  the same XML locally reconstructed it as completed, accepted the entry and could exit zero.
  The same assumption could declare absent mutants stale from an unfinished generated set.
- Root cause: `mutant_record` defaulted to completed and `report_records` discarded the lane's
  existing completion metadata. The checker independently rebuilt class completeness from XML
  presence and a CLI sampling flag. Record construction now defaults to unknown; one shared
  loader reads completion and sampling from the artifact's existing `results.json` for the
  canonical module report/census paths. The checker uses that provenance for matched entries
  and class completeness, and fails unfinished or unknown reports. Remap consumes the same
  records without claiming completeness. Stream records need no redundant unfinished assignment;
  live XML attachment supplies its known run state, final lane judgment also withholds acceptance
  if later census validation failed, and scheduled stale checks exclude partial module results.
- Why tests missed it: local checker fixtures supplied bare XML and implicitly treated file
  presence as a successful process exit; they tested statuses and census validity without carrying
  the completion evidence the lane already wrote.
- Regression tests: `RefactorHelperTest.test_check_preserves_lane_completion_for_both_disposition_kinds`
  checks actual lane output from failed/completed runs through the CLI. Companion cases cover
  unknown/missing/malformed/duplicate metadata, sampled runs, partial runs, detected-status rules,
  withheld stale judgments and mixed-module input in either order. Existing positive local-check
  fixtures now include explicit completed artifact metadata rather than relying on XML presence.
  Focused command: `python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
  All five new regression cases rejected the starting reporter; all 246 focused tests passed
  after the fix, including the existing lane, local checker and remap contracts.
- Lesson: [default unknown evidence to unproven](../LESSONS.md#mutation-evidence-contracts) and
  transport existing provenance across serialization boundaries as well as in-memory judgment.
- Vision check: consistent verification claims protect confidence in the engine's results and
  cost; there is no product behavior or default change.


## Follow-up: complete target scope and valid mutation-only output reuse

- Fix / issue: review findings R13 and R14; the later R14 decision supersedes withdrawal of the
  mutation consumer's cache reuse, while keeping every other consumer out of scope.
- Symptom and trigger: a completed module targeting Foo and Bar could emit a killed Foo mutant
  and no Bar mutant. The lane called a re-confirmed Bar entry stale, while the local checker
  rebuilt scope from emitted records and reported not judged. Separately, the required reuse of
  Build's compilation remained unimplemented after the earlier cache claim was withdrawn.
- Root cause: R13 loaded completion but discarded target membership. The same metadata boundary
  now loads validated `targets` with their completeness; both XML and census consumers merge that
  scope, including silent targets, instead of deriving it from emitted identities. R14 omitted
  Error Prone in the mutation consumer, changing compiler arguments and annotation processors.
  MutationTesting now sets the same project property as Build and restores project task history
  beside the existing output archive. A separate history key avoids changing the shared output
  cache's path/version contract or duplicating its large output payload for other consumers.
- Why tests missed it: local fixtures had one selected class and inferred scope from at least one
  emitted identity. Cache assertions had no matching-input proof. The current follow-up explicitly
  leaves core/query tests, Sonar, REST and platform compiler/cache policy unchanged; matching the
  other output consumers to Build remains follow-up work. Platform jobs currently compile cold.
- Regression tests: `RefactorHelperTest.test_check_and_lane_judge_a_completed_silent_target_stale`
  compares actual lane output with local judgment for a silent completed target; a companion test
  obtains the same silent target solely through census-module metadata. Invalid target metadata
  cannot prove staleness. Focused verification covers the reporter tests, parsed workflow cache
  producer/consumer contracts and Gradle JavaCompile configuration under both producer and mutation
  settings. CI task outcomes from this PR remain the evidence of actual restored compilation;
  no time-saving figure is claimed. Both silent-target regressions rejected the starting
  reporter, and all 249 focused Python tests passed. Parsed workflow checks confirmed matching
  compiler properties, paired history cache keys/paths and unchanged other jobs. Gradle
  configuration comparisons matched compiler arguments, toolchains, fork settings and dependency
  declarations across 20 JavaCompile task models; this did not measure restored task outcomes.
- Lesson: [carry selected scope with evidence provenance](../LESSONS.md#mutation-evidence-contracts),
  and compare compiler inputs before claiming reuse of compiled outputs.
- Vision check: these changes preserve trustworthy verification at bounded cost, without changing
  engine behavior or defaults.


## Follow-up: run completion is not artifact completeness

- Fix / issue: review finding R15, including correction of two incorrect test oracles.
- Symptom and trigger: a completed restricted module's XML alone could declare an off-diff
  census identity stale; its census alone could declare a reported identity stale. Omitting a
  census also discarded the lane's recorded validation failures. Two local helper tests wrongly
  demanded stale judgments without the required census.
- Root cause: separate consumers inferred completeness from module completion plus the artifacts
  they happened to hold. `ModuleEvidence` now owns supplied/read-valid artifact flags, module
  completion, selected targets, parsed identities and recorded failures. One `completely_loaded`
  predicate feeds acceptance and staleness; malformed/unreadable or count-inconsistent components
  cannot establish completeness. The helper never calls the lane's empty-census reader for a
  missing path. Producer-recorded zero counts are explicit empty-component evidence, distinct
  from an arbitrary missing helper file. The previous per-file completeness helpers are removed. Invalid census files with unknown module ownership invalidate selected
  modules at the same loading boundary, because their unreadable identities cannot be attributed
  safely; the initial focused run caught that missing propagation in four existing assertions.
- Why tests missed it: fixtures conflated a complete run with a completely supplied generated
  set and encoded that mistake as an expected stale result. Complete-pair, missing-pair and
  sampled cases now use CONV-202 rule 5 as the oracle: missing evidence cannot prove absence.
- Regression tests: corrected `test_check_lists_stale_and_re_confirmation_entries_with_their_action`
  and `test_check_reaches_the_lane_states_when_it_is_given_the_lane_census` now require not judged
  without the census, retain the off-diff match with it, and assert stale only from a complete
  nonmatching pair. Additional cases cover census without its nonempty report, missing paths,
  omitted recorded failures, acceptance of both kinds only with a complete pair, unreadable and
  malformed inputs, mismatched counts, and sampled incompleteness. Existing positive repository
  fixtures now regenerate metadata when replacing their report contents. A final symmetric
  case distinguishes a missing nonempty report from the producer-recorded zero report, including
  absent, zero-byte and well-formed empty XML forms.
  Focused command: `python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
  Six corrected-oracle and missing-component regressions rejected the starting reporter. The
  final focused suite passed all 255 tests after the ownership and zero-report follow-ups.
- Lesson: [load the entire generated set before judging absence](../LESSONS.md#mutation-evidence-contracts)
  and validate test oracles against the contract rather than preserving an earlier expectation.
- Vision check: the change keeps verification claims about engine results and cost trustworthy;
  there is no product behavior or default change.


## Follow-up: artifact identity must precede grouping

- Fix / issue: review finding R16.
- Symptom: an absolute report path and relative census path for the same completed restricted
  module created two half-complete evidence values. The checker reported an unmatched existing
  entry as not judged and could exit zero where a fully loaded generated set proves it stale.
- Root cause: the loader deduplicated and grouped unresolved path spellings. Both collections
  now resolve against the working directory before either operation. The same grouped value
  consequently reaches completeness, survivor acceptance and staleness; remap shares this boundary.
- Why tests missed it: complete-pair fixtures used matching absolute forms, and repeated-report
  tests used identical spellings rather than two spellings of the same artifact.
- Regression tests: `test_mixed_artifact_path_forms_keep_the_stale_verdict` checks all four
  absolute/relative pairings against CONV-202 rule 5. Companion cases verify acceptance for both
  disposition kinds in each direction and one failing-mutant record when both path forms are
  supplied together. They execute the checker against actual lane artifacts and assert labels,
  failure status and output multiplicity, without inspecting implementation source.
  Focused command: `python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
  All three new regressions rejected the starting implementation; the fixed reporter passed
  all 258 focused tests.
- Lesson: [canonicalize identity before grouping evidence](../LESSONS.md#mutation-evidence-contracts).
- Vision check: correct grouping preserves trustworthy verification of engine results and cost;
  there is no product behavior or default change.


### Review round 8: reject completion claims together

- Failure: a zero-exit module with a changed-line census violation still accepted and counted
  a surviving mutant's disposition. An empty XML report could also become `off_diff_only`,
  contradicting the changed-line identity in that same census. The overall failure concealed
  incorrect published evidence.
- Root cause: module state, artifact coverage and per-record provenance implemented separate
  completeness decisions. Earlier fixes repaired individual consumers without enumerating the
  claims that depended on the shared invariant.
- Correction: one module evidence value names all rejecting facts: unfinished execution,
  unreadable report/census, malformed census records, changed-line violations, missing artifacts,
  per-class sampling and invalid recorded evidence. All six completion judgements consult that
  value: acceptance, both dispositioned counts, staleness, empty-scope promotion and rule 1 status
  provenance. The lane records its rejection facts for the helper; entries name the reason their
  judgement was withheld. Summary scope wording describes the request without claiming the
  evidence is complete. Rule 4 still contradicts or supersedes entries from preserved statuses.
- Regression oracle: CONV-202 rules 1, 4 and 5. `CompletionEvidenceTest` exercises every rejecting
  fact against every dependent judgement, pairs all judgements with valid evidence, checks rule 4
  for every rejection and both disposition kinds, executes the checker against rejected artifact
  pairs, and reproduces the violating-census survivor and empty-report cases through the lane.
  A count-mismatched XML fixture retains observed statuses while failing a complete-read claim;
  missing artifacts cannot prove absence. No test reads implementation source as evidence.
- Why tests missed it: previous timeout tests proved overall failure and local-checker validation
  independently; neither required every completion-based claim to be withheld for every rejection.
- Verification: the four lane regression cases rejected the starting implementation.
  All 263 focused reporter tests passed; after correcting the summary scope wording, all 23
  reporting and completion-evidence tests passed. Commands:
  `python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'` and
  `PYTHONPATH=.github/scripts/tests python3 -m unittest test_pr_mutation.ReportingTest test_pr_mutation_dispositions.CompletionEvidenceTest`.
- Lesson: [enumerate evidence rejection facts and dependent claims](../LESSONS.md#mutation-evidence-contracts).
- Vision check: truthful verification evidence supports confidence in engine behavior and cost;
  no product behavior or default changes.


### Review round 10: preserve selection and own reused workers

- R18 failure: merging three 25-test package scopes into one invocation selected only 50 of the
  75 tests the former partitions selected. Existing coverage could become `NO_COVERAGE`.
- R18 root cause: a per-invocation union cap was carried into the single-pass design. Selection
  now checks each source's candidate limit before unioning all admitted candidates. Named,
  related, package and association rules, package fallback limits and individual over-limit
  marking remain intact. Both PR and scheduled callers share this boundary.
- R18 regression oracle: CONV-205 forbids changing test selection when merging coverage passes.
  `SelectionTest.test_single_invocation_preserves_the_partition_selection_union` compares the
  single invocation to two partition selections and an independent literal 75-test inventory,
  with and without a 61-candidate source that remains over-limit. Exact-limit and association
  cases retain coverage of individual candidate limits.
- R19 failure: a later launcher connected to a daemon from an earlier module. On timeout,
  walking only the new launcher's descendants missed the daemon and its PIT workers, and the
  runner could read artifacts or launch another module while those processes were still active.
- R19 root cause: lifetime reuse severed process ancestry as an ownership proof. A Gradle init
  script now publishes an atomic serving-daemon PID receipt for every invocation; the executor
  clears stale receipts before launch. Timeout cleanup stops the owned parents before discovering
  descendants, kills that stopped tree and waits until every owned process is absent or a zombie
  (terminated, no executable work or resources) before returning. Successful runs keep the daemon.
- R19 regression oracle: the documented hard deadline and CONV-205 require completed termination
  before artifact consumption or the next module. `ReusedDaemonTimeoutTest` uses a real reused
  daemon outside the launcher tree and a detached worker. It checks termination immediately on
  return, real artifact reads, a subsequent module's start, and rejection of a stale PID receipt.
  The fixture simulates the init script's PID receipt; it proves the executor enforcement
  boundary, not integration against an actual Gradle distribution.
- Why tests missed these defects: selection tests asserted the union cap rather than preservation
  across merged passes; the old timeout test created a detached child of the current launcher,
  so it did not exercise daemon reuse across launchers or completed cleanup ordering.
- Verification: the selection-union regression and both reused-daemon regressions rejected the
  starting implementation. All 269 focused mutation-tooling tests passed. An obsolete fake-PID
  test that mocked only process-group signaling was replaced with a real launcher termination
  and preserved-log check. Command:
  `python3 -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`.
- Lesson: [selection unions and worker ownership](../LESSONS.md#mutation-evidence-contracts).
- Vision check: preserving coverage and enforcing the stated execution budget supports trustworthy
  evidence about engine correctness and cost. The union-cap removal implements the recorded
  maintainer decision; no product behavior or default changes.
