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
  between producer and consumers. All six new task-history cache entries and the associated
  recompilation claim are withdrawn, retaining one invocation per module and daemon reuse.
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
