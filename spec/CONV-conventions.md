# CONV — Conventions and conformance

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 document conventions, traceability rollout, mutation dispositions, test associations, mutant scope and the scheduled full-scope report for the PR mutation lane (CONV-201 to CONV-206), the accepted work-budget classification, and the timed acceptance verdict with its results ledger and nightly trend tier (CONV-302 to CONV-304). The mutation requirements are implemented in the CI reporter, its PIT interceptor plugin and the scheduled workflow; the proposed trace tool remains follow-up work. Code fields for process requirements point to the authoritative workflow artifacts; no product method enforces them.

## Requirements

### CONV-101 — Normative statements and stable identity

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [CONTRIBUTING.md](../CONTRIBUTING.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** SpecTraceContractTest: parse chapters; reject duplicate/malformed/retired IDs, missing metadata and dangling citations.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** The existing NATIVE chapter supplies the repository convention. Automated trace tooling is not delivered here.

Every normative requirement MUST have a unique `CHAPTER-NNN` ID, Level, Status, Since, Code, Verified by, Evidence class and Gap/note. IDs MUST NOT be reused or renumbered; retirement records the reason in `RETIRED.md`. Only uppercase RFC 2119/8174 keywords inside requirement blocks (and tables explicitly incorporated by them) are normative. Unresolved questions carry `open` and impose no chosen behavior.

### CONV-102 — Conformance and evidence honesty

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [CONTRIBUTING.md](../CONTRIBUTING.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** SpecTraceEvidenceTest: reject an enforced mapping with no passing JUnit result; accept it only with a default-lane result for the same baseline.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** No P0 requirement is promoted to enforced by source inspection in this change. CI result attribution remains follow-up work.

Conformance to an adopted chapter MUST satisfy every decided MUST, including requirements with incomplete implementation or verification. Status MUST NOT weaken the target. An `enforced` claim requires an implemented behavior and a mapped deterministic test that passed in a default CI lane; skipped, disabled and assumption-aborted tests do not count. A planned test MUST be labelled planned.

### CONV-103 — Specification before implementation

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [CONTRIBUTING.md](../CONTRIBUTING.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** Review acceptance: requirement-to-test-to-change mapping, PIT artifact with survivor dispositions; for docs-only changes, links, IDs, traceability and evidence audit instead of artificial application tests.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** Process contract anchored in ../CONTRIBUTING.md and ../docs/VERIFICATION.md; no product behavior changes here.

A feature or behavioral fix MUST state behavior, invariants and errors before product implementation and derive tests from those requirements before or independently of the implementation. Each test oracle MUST distinguish at least one plausible violation independently of implementation details. Changed logic MUST receive mutation testing. Each surviving or uncovered mutant MUST be resolved in exactly one of four ways: (1) a behavior test, derived from the requirements, that detects it through observable behavior, including the lane's selection of an existing one (CONV-204); (2) an `equivalent` disposition (CONV-201); (3) a `killed-out-of-lane` disposition, when only a test that cannot run under mutation instrumentation observes it (CONV-201); or (4) removal of the mutated code, when the mutant shows that code has no observable effect. Tests are derived from requirements; they MUST NOT be written to cover lines or kill mutants for their own sake (CONV-202). A test that detects a mutant only by inspecting private state, internal call counts or other implementation details that no requirement specifies MUST NOT count as resolution (1). Bug fixes MUST include the postmortem template, regression and lesson required by CONTRIBUTING.

### CONV-104 — Traceability rollout

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [CONTRIBUTING.md](../CONTRIBUTING.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** SpecTraceContractTest plus fixtures for each failure, annotation discovery, default-lane versus opt-in evidence and PIT selection by requirement.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** spec_trace.py, @Verifies, traceability CI wiring and test annotations remain planned follow-up work.

The traceability tool MUST report unmapped MUSTs, unknown or retired test IDs, stale generated mappings, skipped-only evidence, duplicate IDs and dangling citations. `@Verifies` MUST name the requirement and evidence kind. Initial rollout MUST be report-only; making checks blocking requires the staged adoption described below, without silently waiving old gaps.

### CONV-105 — Compatibility and versioning

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [NATIVE-embedded-c-api.md](../spec/NATIVE-embedded-c-api.md); [CONTRIBUTING.md](../CONTRIBUTING.md)
- **Verified by:** No existing test mapped. **Planned:** SpecChangeCompatibilityTest and release review: compare the frozen requirement corpus and API/format baselines, reject an unclassified incompatible change.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** This is pre-freeze specification work, not a declaration that the current development bytes or every public class are already frozen.

Spec 1.0 MUST accompany product 1.0.0. A frozen requirement change that removes a guarantee or rejects previously valid use MUST be classified as breaking and require a major release; additive compatible behavior is minor and editorial repair is patch. Each behavioral change MUST update its requirement and CHANGELOG in the same change.

### CONV-201 — Mutation disposition record

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) `parse_dispositions`, `entry_problems`, `evidence_problems`; [mutation-dispositions.toml](../.github/mutation-dispositions.toml)
- **Verified by:** [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `DispositionRecordTest` (both kinds, every missing/unknown/empty/mistyped field, pattern characters per identity field, duplicates, unknown kinds, justification floor, counterfactual evidence form, malformed documents).
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** The identity fields are PIT 1.25.5 `mutations.xml` elements; across 170 saved Sirix PIT reports (40,054 mutants) the six-field identity had no collision, dropping the instruction indexes caused 23,853 collisions and replacing them by the line number 4,640, and 399 of 512 identities shared by two revisions of one change kept their identity while their line moved. The boundary exception to the cost rule is judged in review (CONV-203); the reporter validates the justification's form, not its cost argument.

A disposition MUST be an entry in the repository file `.github/mutation-dispositions.toml`, a TOML document whose only top-level key is `mutant`, an array of tables; an absent file MUST mean no entries. Each entry MUST have exactly the fields its kind requires, each non-empty; an unknown, missing, empty or mistyped field MUST make the whole file invalid.

| Field | Content | Kinds |
|---|---|---|
| `class` | `<mutatedClass>`: the JVM binary name, e.g. `io.sirix.Foo$Bar` | both (identity) |
| `method` | `<mutatedMethod>`, e.g. `put`, `<init>`, `lambda$put$0` | both (identity) |
| `descriptor` | `<methodDescription>`: the JVM method descriptor | both (identity) |
| `mutator` | `<mutator>`: PIT's fully qualified mutator id | both (identity) |
| `indexes` | the integers of `<indexes>`, in report order: PIT's instruction ordinals within the method | both (identity) |
| `description` | `<description>`, compared as exact text | both (identity) |
| `kind` | `equivalent` or `killed-out-of-lane` | both |
| `checked_at` | full 40- or 64-hex commit ID whose PIT report the author inspected | both |
| `justification` | the argument the kind requires (below) | both |
| `test` | the detecting test as `<binary test class name>#<method name>` | `killed-out-of-lane` |
| `evidence` | the counterfactual record (below) | `killed-out-of-lane` |

The six identity fields form the mutant's identity, PIT's own mutation identifier (class, method, descriptor, instruction indexes, mutator) plus its description. An entry MUST match a mutant only when all six are equal as exact strings and integer lists. Fields MUST NOT be interpreted as patterns. A value not well formed for its field MUST make the file invalid: `class` and `mutator` MUST be dot-separated Java identifiers (`$` allowed), `method` a JVM unqualified method name (`<init>`, `<clinit>` or a name without `.`, `;`, `[`, `/`, `<`, `>`), `descriptor` a JVM method descriptor, and `indexes` non-negative integers; a glob, regular-expression or wildcard character (`*`, `?`, `[`, `]`, `{`, `}`, `|`, `\`) MUST make the file invalid in every identity field except where the descriptor grammar requires `[` for an array type. Line numbers MUST NOT be part of the identity, so an entry survives edits that only move the mutant's line; an edit of the same method that changes the mutant's instruction ordinal or description makes the entry stale (CONV-202). Two entries with the same identity MUST make the file invalid. `checked_at` records evidence provenance only; the reporter MUST validate its form, not its ancestry, because merge strategies may make it unreachable.

**`equivalent`.** `justification` MUST argue, from the specification, the public contract or a documented invariant, that no observable behavior differs between the original and the mutated program for any input the contract admits. Observable behavior includes results, errors, persisted state, and cost: allocation, work and latency. A mutant that changes only cost is therefore never equivalent, with one narrow exception: a mutant that produces the same result and differs only at a single boundary input, by at most one extra constant-time step, MAY be recorded as `equivalent` when its justification states exactly that. Every other cost-only mutant MUST be detected by an allocation, work-budget or latency test, in the lane where that test can run, or else dispositioned as `killed-out-of-lane`; a cost that no test bounds MUST receive a budget (CONV-301) or its code MUST be removed. A justification MUST NOT argue from test difficulty or test selection. A mutant that a deterministic test detects, including a test the lane did not select, is not equivalent: it is a selection miss, resolved through CONV-204 or a focused rerun, never by an entry.

**`killed-out-of-lane`.** This kind is reserved for a mutant whose only observable effect is checked by a test that cannot run under PIT instrumentation, such as a clean-JVM allocation budget or a latency gate. `test` MUST name that test, and `justification` MUST state which bounded effect the mutant changes and why the test cannot run in the lane. `evidence` MUST be a table with exactly these fields: `patch`, a unified diff that injects exactly this mutant into existing source files in place (each old and new header MUST name the same repository path; creation, deletion, rename, copy, binary and file-mode operations MUST be rejected, including header-only Git sections); `command`, the argument list that runs the test in its own clean process; `baseline_exit`, the exit status of that command on the unmodified sources, which MUST be 0; `injected_exit`, its exit status with the patch applied, which MUST be non-zero; `failure`, the failing assertion's output excerpt; and `sources`, the repository path and SHA-256 of every file the patch touches, taken before injection and verified identical after the patch was reversed. The reporter MUST validate the form of this record; the evidence is produced by the local counterfactual helper (CONV-203), and review judges whether `patch` is the exact mutant.

Both kinds' justifications are checked by a floor: the reporter MUST reject a justification shorter than 60 characters after whitespace normalization or containing, case-insensitively, `hard to test`, `difficult to test`, `untestable`, `not testable` or the words `TODO`, `TBD` or `FIXME`; review (CONV-203) judges the argument. A mutant that shows the mutated code has no observable effect at all MUST be resolved by removing that code (CONV-103 resolution (4)), not by an entry.

### CONV-202 — PR mutation verdict with dispositions

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) `judge_entry`, `judge_dispositions`, `apply_dispositions`, `is_clean`, `read_stream`, `resolve_empty_scope`, `read_class`; [StreamingResultListener.java](../tools/pit-changed-lines/src/main/java/io/sirix/pit/StreamingResultListener.java), [StreamingCompletionAgent.java](../tools/pit-changed-lines/src/main/java/io/sirix/pit/StreamingCompletionAgent.java), [CompletionOrderAnalysis.java](../tools/pit-changed-lines/src/main/java/io/sirix/pit/CompletionOrderAnalysis.java); [gradle.yml](../.github/workflows/gradle.yml)
- **Verified by:** [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `VerdictTest` (end-to-end `run` over fixture reports, class images and files: both kinds reported apart, NO_COVERAGE and other statuses never dispositioned, line-shifted and near-miss identities, unconfirmed/contradicted/superseded/stale/not-judged/inactive entries, re-confirmation by changed method only, invalid files, incomplete runs) and `MergeBaseFileTest`; `CensusVerdictTest` (a census identity judges an entry without supplying a status, a censused changed-line mutant fails the run, an incomplete generated set judges no staleness) and `PartialResultTest` (a module killed by its deadline reports every class the stream recorded, stays incomplete and proves no complete generated set; unfinished-run survivors authorize neither disposition kind, while completed-report acceptance and rule 4 remain intact) and `CompletionEvidenceTest` (each named rejecting fact crossed with all six completion judgements, valid controls, rule 4 and checker parity) and `EmptyScopeTest` (a census-explained empty report is complete and clean; an unexplained, failed or killed one stays incomplete); [StreamingResultListenerTest.java](../tools/pit-changed-lines/src/test/java/io/sirix/pit/StreamingResultListenerTest.java) (one PIT-shaped element per mutant, flushed per analysed class, escaping that keeps one record per line); [StreamingDeadlineTest.java](../tools/pit-changed-lines/src/test/java/io/sirix/pit/StreamingDeadlineTest.java) (real PIT preserves later completed units behind a blocked leading unit; completed runs emit all expected mutants once and retain ordinary XML output); `ClassReaderTest`; reader differential against `javap` on 300 compiled core classes recorded in the PR.
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** Extends the existing [blocking criterion](../docs/VERIFICATION.md#blocking-criterion-the-prs-own-lines), whose changed-line attribution, incomplete-analysis rules and use of PIT's `detected` attribute (`TIMED_OUT`, `MEMORY_ERROR` and `RUN_ERROR` count as detected) are unchanged. Applies to the PR lane only; the weekly curated core PIT task and the scheduled full-scope report (CONV-206) read no dispositions for a verdict. The lane no longer samples: CONV-205 restricts generation to the changed lines and forbids a per-class limit, which retires the caveat that a reported count below a limit might still be a sample, and makes every changed-line count a complete measurement. What the restriction removes is still listed by identity in the census, so an entry keeps a cheap existence proof, but no status: an entry on an unchanged line can therefore be shown non-stale and can be re-identified after a refactor, while whether its mutant still survives is answered by a focused local run or the scheduled report. PIT applies its own equivalence filters (`FILTER`) after the restriction, so a census identity may name a mutant a full run would never report; that makes census-based staleness verdicts conservative, never falsely stale. Re-confirmation by changed method (rule 2) is unchanged.

Tests MUST NOT be written to cover lines or to kill mutants for their own sake. A surviving or uncovered mutant is a signal about the specification and its behavior tests, resolved as CONV-103 lists. In particular a changed line that no selected test executes (`NO_COVERAGE`) MUST be resolved by exactly one of: (1) the specified behavior that needs this code receives a behavior test derived from its requirement, not aimed at the line, which then covers it; (2) the code has no specified behavior and is removed; (3) existing behavior tests do cover it but the lane did not select them, and a CONV-204 association makes the lane run them. A `NO_COVERAGE` mutant MUST NOT be dispositioned by either kind, because a mutant that never executed has shown nothing.

Definitions for one lane run. A *changed-line mutant* is a reported mutant attributed to a line the PR changed, as defined by the existing blocking criterion. A method is *changed* when a line of its compiled line-number table is a changed line of its source. An entry is *added* when the dispositions file at the merge base of the PR's base and checked-out revisions contains no entry equal to it in every field, and *pre-existing* otherwise; an added entry is a *re-confirmation* when the merge-base file holds an entry with the same identity, and *new* otherwise. If the merge-base file is absent or invalid, every entry MUST be treated as new. A *mutated class* is a class in the `targetClasses` of a module whose run completed. The *restriction census* of such a module lists, by identity, the mutants its changed-line restriction removed before analysis (CONV-205); a census identity carries no status, because its mutant was generated and never run. A mutated class's *generated set* is what its report and its census list together, and it is *complete* only when the run applied no per-class mutant limit to that class. Statuses a module preserved after its deadline killed it belong to its report; they prove no generated set complete.

Completion-based judgements MUST share one named evidence decision. Its rejection facts MUST include an unfinished module, an unreadable report, an unreadable census, malformed census records, changed-line census violations, missing required artifacts, and a per-class mutant limit. Any such fact MUST withhold added-entry acceptance, both dispositioned counts, generated-set staleness, empty-scope eligibility, and status provenance for rule 1. Rejected evidence MUST retain its named reasons in artifacts and entry actions; rule 4 MUST continue to use every preserved status. The lane, local checker and full-scope staleness advisory MUST use that same decision.

The run MUST be clean only if the existing blocking criterion's completeness conditions hold, the dispositions file is valid (CONV-201), and every one of the following holds:

1. Every changed-line mutant is detected, or has status `SURVIVED` and is matched by an added entry. A changed-line `NO_COVERAGE` mutant MUST fail even when matched, and so MUST every other undetected status.
2. No pre-existing entry belongs to a changed method. Such an entry MUST fail as *needs re-confirmation*: the author re-reviews the argument (and, for `killed-out-of-lane`, re-runs the counterfactual) and updates the entry, at least its `checked_at`. Entries of methods the PR did not change MUST NOT need re-confirmation, whatever else changed in their file.
3. Every new entry matches a mutant this run generated: a `SURVIVED` mutant of its report, or an identity of its census. A new entry that matches neither, and any added entry that matches a reported undetected mutant whose status is not `SURVIVED`, MUST fail as *unconfirmed*; matches to reported detected mutants follow rule 4. Since only changed sources are mutated, entries cannot be added for code outside the PR's changed files; an added entry whose only match is a census identity MUST be reported as *not judged* without failing, because the run measured no status for it, which lets an author re-identify an entry after a refactor (CONV-203), and the summary MUST mark such matches as censused off the diff.
4. No `equivalent` entry matches a `KILLED` mutant: it MUST fail as *contradicted*, because a detecting test refutes the claim. No `killed-out-of-lane` entry matches a detected mutant: it MUST fail as *superseded*, because an in-lane test now detects it. An `equivalent` entry matching `TIMED_OUT`, `MEMORY_ERROR` or `RUN_ERROR` MUST be reported, not failed, because those statuses can arise from timing or infrastructure.
5. No entry is *stale*. An entry MUST be stale when its class is absent from every module's compiled main classes, its method name and descriptor are absent from that class, or, for `killed-out-of-lane`, its test class is no longer declared in any module's test sources, all checked for every entry in every run; or when its class is a mutated class whose generated set is complete and neither a reported mutant nor a census identity of that class matches it.

A re-confirmation that matches no mutant of a class whose generated set is incomplete or that this run did not mutate MUST be reported as *not judged*, without failing, and so MUST a pre-existing entry in such a class. A pre-existing entry of an unchanged method that matches a reported mutant with a non-dispositionable undetected status MUST be reported as *inactive*, without failing. An entry whose only match is a census identity MUST be reported as not judged and MUST NOT change the outcome, whether it is added or pre-existing. Under CONV-205's scope every reported mutant lies on a changed line and therefore in a changed method, so a pre-existing entry that matches a reported `SURVIVED` mutant is a rule 2 re-confirmation rather than an advisory disposition; the scheduled full-scope report (CONV-206) is where an unchanged-line survivor is measured again.

The job summary and `results.json` MUST keep the two kinds apart and list, per module: the changed-line mutants killed, otherwise detected, dispositioned as equivalent, dispositioned as killed out of lane (with the named test) and failing; each dispositioned mutant with its matching entry's `checked_at` and justification (the summary MAY truncate the justification; `results.json` MUST NOT); each failing mutant with its reason (no entry, not dispositionable status); and every entry in a failing or reported state (needs re-confirmation, unconfirmed, contradicted, superseded, stale, not judged, detected-other) with its identity, its position in the file and the action that resolves it, naming the local helper command for stale and re-confirmation states. The summary and `results.json` MUST also state, per module, how many mutants the restriction removed and where the report on those mutants lives (CONV-206), and MUST NOT present a census count as a mutation score. A census record the reporter refuses MUST be reported with the reason it was refused, naming the field at fault, because the restriction wrote that record and a defect in it has to be findable; the reported reasons MAY be bounded, and the bound MUST then say how many it left out. Validation errors MUST name the entry's position in the file and the violated rule. All file-derived text MUST be escaped as existing summary text is.

A report with no mutant is incomplete unless the run's census explains it: a run that executed, generated mutants for its target classes and had none of them on a changed line MUST be reported as complete, with a changed-line mutant count of zero and no disposition claim, because there is nothing to judge rather than nothing to measure. A report with no mutant and no census entry MUST stay incomplete, as MUST one from a run that failed or was killed.

A module whose analysis its deadline killed MUST report every mutant status recorded before the kill. The lane MUST take those statuses from a result stream the analysis flushes as each analysed class completes, not from an end-of-run report, and MUST still report the module as incomplete. When that stream cannot be read, the module MUST keep the state its deadline gave it and MUST report, beside it, why nothing was recovered. Preserved statuses MUST be used by the status rules (rule 4) and the summary, and MUST NOT make an incomplete module clean, satisfy rule 1, or prove a generated set complete. An added entry matched only by a preserved `SURVIVED` status MUST be reported as *not judged*, with its unfinished-run provenance stated, and MUST NOT contribute to either dispositioned count. This applies to statuses recovered from the stream or read from XML after a failed or timed-out run.

### CONV-203 — Disposition review, abuse limits and refactoring

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) `format_dispositions`, `build_parser`, `check_dispositions`, `remap_dispositions`, `counterfactual`; [VERIFICATION.md](../docs/VERIFICATION.md#reviewed-dispositions)
- **Verified by:** [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `ReviewTest` (summary counts and full added claims, escaping, CI commands without disposition input, removal monotone over every subset), `RefactorHelperTest`, `CounterfactualTest`. Review acceptance for the reviewer obligation; the PR records disposition churn measured on a sample refactor.
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** Precedents compared: PIT `excludedMethods`/`excludedClasses`/`avoidCallsTo` and its `@Generated`/`@DoNotMutate` filter remove whole methods or classes before mutation, invisibly in results; Stryker's `// Stryker disable next-line <mutators>: <reason>` comments keep an optional reason in the diff but cover a source line and a mutator list, not one mutant; mutmut's and cosmic-ray's `# pragma: no mutate` cover whole lines, and cosmic-ray's operator filter covers operators by regular expression. This requirement keeps their visibility in review and rejects their breadth: exact per-mutant identity, a mandatory reason, re-confirmation when the method changes, and no numeric cap or label, for the reasons stated in the requirement.

Dispositions are expected to be rare: behavior, budget and latency tests detect most mutants, and an entry records a reviewed exception, not a way to finish a PR. A disposition MUST apply to one mutant identity; class-, method-, mutator-, line- or file-wide waivers MUST NOT exist in the file, the reporter or the workflow. Dispositions MUST come only from the dispositions file of the checked-out revision and of the merge base; the reporter MUST NOT accept dispositions or a relaxation of CONV-202 from command-line flags, environment variables, PR labels, comments or artifacts. The job summary MUST open its disposition section with the number of added and removed entries and MUST list every added entry with its kind, identity and full justification, so a reviewer sees each claim beside the diff. A reviewer approving a PR that adds or changes entries MUST check each justification against CONV-201's standard. No per-PR numeric cap and no approval label applies: a cap would push authors toward line-targeted tests that CONV-202 forbids, and a label would need a re-run on every label event. Removing an entry MUST always be permitted; it can only withdraw dispositions and MUST NOT turn any undetected changed-line mutant from failing into accepted.

A pure refactor MUST stay cheap. The reporter MUST offer local helpers that CI never runs: a read-only *check* that, given a PIT report, the compiled classes and optionally a restriction census (CONV-205), lists stale, re-confirmation, unconfirmed and superseded entries with the action for each, applying the same census rules as the lane's verdict so a local judgement of a lane report matches it; a read-only *remap* that, for each stale entry, proposes candidate identities from the report with the same mutator and description, preferring the same method with other instruction indexes, then a renamed method of the same class, then a method of the same name and descriptor in another class, and prints every eligible candidate once in tier order, labelling each with its tier and whether its instruction indexes match, for the author to review without writing the file; a lane report offers candidates only on the PR's own lines, so an author remapping an entry whose mutant lies elsewhere MUST be able to supply a report from a run without the restriction (CONV-205) and MUST be told, when a candidate came from a census rather than a report, that no status was measured for it; and a *counterfactual* runner that hashes the touched sources, checks that the test passes on them, applies the injection patch, runs the test in its own process, reverses the patch, verifies the hashes and prints the `evidence` table, refusing to print one when the baseline fails, the injected run passes or a hash differs. No helper output MUST be applied to the dispositions file automatically.

Malformed, unreadable or changed-line-violating census evidence MUST appear in the checker output and fail its exit status, and MUST NOT prove a generated set complete. Classes represented only by valid census identities MUST be included in the checker's judged scope. The local evidence loader MUST recover each report and census file's module completion and sampling provenance from the lane artifact's existing `results.json`, using the fixed module-relative artifact paths. XML presence alone MUST NOT imply completion. Missing, malformed, ambiguous or unassociated completion metadata MUST withhold acceptance and generated-set completeness; an unfinished or unknown report MUST make the check fail while preserving rule 4 judgments. A completed module's evidence MUST NOT confer completion on another module's report. Target-class membership and generated-set completeness MUST be loaded together from the module's recorded `targets`, including targets with no emitted mutant; neither XML nor census identities alone define the selected target set. The loader MUST represent each module's supplied report and census, the validity and completeness of each read, module completion, and recorded census failures together. Both artifact collections MUST be resolved against the working directory before deduplication and module grouping, so path spelling cannot change acceptance, staleness or evidence completeness. One generated-set loading predicate MUST require every nonempty component of a completed restricted run to be supplied and valid; completion metadata alone MUST NOT make omitted identities absent. A missing census path in a helper is not supplied, unlike the lane's known empty census after deleting prior output before execution. A producer-recorded zero count MAY attest that a component has no missing identities. Report-only and census-only incomplete input MUST withhold acceptance and absence-based staleness; an unmatched existing or re-confirmed entry is then not judged rather than stale, without failing merely for missing evidence. Recorded census failures MUST still fail the check when the census itself is not supplied. Sampling MUST withhold every completion-based judgement, including acceptance and staleness based on absence.

### CONV-204 — Lane test associations

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) `parse_associations`, `load_associations`, `test_scope`; [mutation-test-associations.toml](../.github/mutation-test-associations.toml)
- **Verified by:** [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `AssociationTest`; [test_pr_mutation.py](../.github/scripts/tests/test_pr_mutation.py) release-contract selection tests read the repository file.
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** Generalizes the reporter's hard-coded `CONTRACT_TESTS`, which the file replaces. Selecting tests by measured coverage of the changed classes would remove the need for associations but needs a per-test coverage map inside the lane's budget (issue 1411); it is recorded follow-up work. A test excluded from PIT's baseline by the Gradle configuration (isolated allocation suites, soak and random property tests) does not run even when associated; its effects are resolved through CONV-201's `killed-out-of-lane` kind.

The repository file `.github/mutation-test-associations.toml` MUST state which existing behavior tests the lane must run for a changed class, and nothing more. Its only top-level key MUST be `association`, an array of tables with exactly the fields `class`, the binary name of a top-level main class declared by a source the lane can select, and `tests`, a non-empty list of binary names of top-level test classes declared in the same module. Names MUST be exact; a pattern character, an unknown or missing field, a duplicate `class`, a class with no declaring main source, or a test not declared in that module MUST make the file invalid, and an invalid file MUST make the run not clean with a message naming the entry and the rule. Ownership MUST follow the declared package and top-level Java/Kotlin types, including Kotlin file facades for top-level functions or properties (the default `<File>Kt`, or the file's `JvmName`), independently of source filenames and directories. Class literals and other member references MUST NOT count as declarations or hide subsequent declarations. When a source is selected, the tests associated with every top-level binary class it declares MUST be added to its candidate tests and mark it resolved even if no name-based rule matched; they MUST count toward that source's candidate limit as other candidates do; the union across sources MUST NOT be truncated.

### CONV-205 — PR mutation lane mutant scope

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ChangedLineInterceptor.java](../tools/pit-changed-lines/src/main/java/io/sirix/pit/ChangedLineInterceptor.java), [ChangedLineInterceptorFactory.java](../tools/pit-changed-lines/src/main/java/io/sirix/pit/ChangedLineInterceptorFactory.java), [ChangedLineMap.java](../tools/pit-changed-lines/src/main/java/io/sirix/pit/ChangedLineMap.java), [RestrictionCensus.java](../tools/pit-changed-lines/src/main/java/io/sirix/pit/RestrictionCensus.java); [pr-mutation.gradle](../gradle/pr-mutation.gradle); [pr_mutation.py](../.github/scripts/pr_mutation.py) `write_changed_lines`, `prepare_analysis`, `read_census`
- **Verified by:** [ChangedLineInterceptorTest.java](../tools/pit-changed-lines/src/test/java/io/sirix/pit/ChangedLineInterceptorTest.java) (every changed-line mutant kept and every other removed, for a source the map does not list and for lines between, before and after the mapped ones; a map the feature cannot read fails the analysis instead of filtering; the declared interceptor type sorts before the type the installed release gives `CLASSLIMIT`, and no `FILTER` of a compound pipeline ever sees an off-diff mutant; census contents, deduplication across passes and durability per class) and [ChangedLineMapTest.java](../tools/pit-changed-lines/src/test/java/io/sirix/pit/ChangedLineMapTest.java) (lookup by package and file name; absent, wrongly headed, empty, short, duplicate and non-positive records rejected); [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `CensusVerdictTest`; [test_pr_mutation.py](../.github/scripts/tests/test_pr_mutation.py) `LaneCommandTest` (map written and passed, census requested, no per-class limit, a single-use daemon with a bounded heap, one PIT invocation per module); `SelectionTest` (single-pass selection equals the former partition union, preserving individual source limits); `ReportingTest` (real single-use descendant termination during startup and analysis, before artifacts and later modules; models Gradle process topology without launching Gradle); [CompletionOrderAnalysisTest.java](../tools/pit-changed-lines/src/test/java/io/sirix/pit/CompletionOrderAnalysisTest.java) (interrupted waits propagate failure with cancellation still observable).
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** Replaces per-class sampling as the lane's cost control, measured on [PR 1353](https://github.com/sirixdb/sirix/pull/1353): 1,098 of 1,132 test-case executions served mutants that could not change the verdict, and three `SURVIVED` return-value mutants on the unchanged `commit()` default methods of `io.sirix.api.NodeTrx` and `io.sirix.api.xml.XmlNodeTrx` took 959 of them. Ordering is asserted against the installed PIT release because PIT documents no interceptor contract: 1.25.5 sorts a compound pipeline by `InterceptorType` ordinal and runs `CLASSLIMIT` as a `FILTER`. The single-parameter rule is asserted the same way, against the release's own feature parser. Removing the limit removes the lane's only bound on mutant count; the bound is now the diff itself, and a diff whose changed-line mutants still exceed the budget reports incomplete analysis rather than a silently narrowed scope. The commercial `+git(scope[line])` add-on provides the same restriction under a licence and is not used.

The PR mutation lane MUST generate mutants only on the lines the PR changed. For every module it analyses, the lane MUST supply that module's changed-line map, as the blocking criterion defines it, to a mutant restriction on PIT's tool classpath. The restriction MUST keep a generated mutant exactly when the map lists its source file together with its line, and MUST remove every other mutant of the analysed classes, including every mutant of a source file the map does not list. A map that is absent, empty, unreadable or malformed MUST fail the analysis: keeping the whole mutant set would spend the budget on mutants no verdict reads, and keeping none would prove nothing while reporting success.

The restriction MUST be applied before any per-class mutant sampling, and a test MUST assert that ordering against the PIT release the lane pins rather than inferring it from documentation. The lane MUST NOT apply a per-class mutant limit at all, so every mutant it generates on a changed line is analysed and a changed-line count is a complete measurement rather than a sample. A focused local rerun MUST disable the restriction by omitting the changed-line map and obtain every mutant of the same classes, including those on unchanged lines; no second override control applies.

For every mutant it removes, the restriction MUST record that mutant's identity (CONV-201) together with its mutated class, source file and line, and MUST record nothing else; that record is the run's restriction census. Where the census is written MUST follow from the changed-line map the lane supplied, not from a second configuration value, and the reporter MUST read it from there: PIT 1.25.5 reads only a feature's first parameter and ignores the rest without an error, so a configured census path would be silently dropped and the verdict would lose its existence evidence. The verdict MUST derive each census entry's attribution from its own changed-line index and MUST fail the run when an entry lies on a changed line, so that a defect in the restriction cannot silently drop a judged mutant. A census entry MUST NOT be reported or counted as a mutant status, and a census count MUST NOT be presented as a mutation score.

A zero-exit run MAY resolve an empty changed-line scope only when its report is absent, zero-byte, or well-formed XML with zero mutation elements, and its census is non-empty and fully readable. Malformed or unreadable report data MUST remain an error even when the census contains off-diff identities. Recovery MUST decode stream and census records independently so a torn UTF-8 tail preserves earlier complete records; unreadable census records MUST retain their refusal reasons.

A PR mutation job consuming the Build job's compiled output MUST use matching compiler inputs, including Error Prone configuration, and restore the producer's project-local task history alongside those outputs. The scheduled job MAY compile its own output without consuming that cache.

Test selection for one invocation MUST equal the union of the per-source selections, preserving named, related and package resolution, the package fallback limit and the per-source candidate limit with its over-limit marking. No limit MUST truncate that union. Each Gradle invocation MUST use `--no-daemon` and `-Dorg.gradle.jvmargs=-Xmx3g`, so its single-use daemon belongs to that launcher. On a deadline, the executor MUST terminate the launcher's process group and detached descendants and wait for termination before reading artifacts or starting another module. Daemon reuse MUST NOT be enabled: it breaks that ownership during startup. One invocation per module and reuse of Build outputs with matching compiler inputs MUST remain intact.

Completed mutation analysis units MUST reach the result stream without waiting for earlier submitted units. A deadline that interrupts an unfinished unit MUST preserve the statuses of later units that already finished, without duplicating records or marking the module complete.
If the completion adapter's wait is interrupted, it MUST propagate failure with the calling thread's interrupt status restored; a failed analysis unit MUST propagate failure without marking the run complete.

Coverage MUST be collected at most once per module per run: the lane MUST NOT execute one module's test scope for more than one mutation scope in the same run. Nothing in this requirement changes mutators, target classes, test selection, time limits or thread count, and the restriction MUST NOT be used to remove a mutant for any other reason than its line.

### CONV-206 — Scheduled full-scope mutation report

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [verification.yml](../.github/workflows/verification.yml); [pr_mutation.py](../.github/scripts/pr_mutation.py) `full_scope`, `window_base`
- **Verified by:** [test_pr_mutation.py](../.github/scripts/tests/test_pr_mutation.py) `FullScopeTest` (no changed-line restriction, census or per-class limit in the command; the window base is the last first-parent commit before the window in a real repository, including merged topic histories, and a window nothing predates is rejected; no verdict in `results.json`; an incomplete module says so and still returns zero; the debt list is not cut short; stale entries advisory, an invalid file judging none); workflow review acceptance that [verification.yml](../.github/workflows/verification.yml) has no pull-request trigger.
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** The window is the report's own parameter, so a class nothing touched within it is absent from the report; that is a narrower claim than "all pre-existing debt" and the report states its window. The weekly curated `:sirix-core:pitest` task is a different measurement (three correctness-critical classes, mutation score of their own suites) and is unchanged by this requirement. No scheduled result exists for this baseline, so the evidence class stays code+test.

The report on mutants outside the PR's own lines MUST be produced by a scheduled full-scope run rather than by the PR lane. That run MUST be reachable only from a schedule or a manual dispatch, MUST NOT run for a pull request, and MUST NOT decide the outcome of any branch or pull request. It MUST mutate its selected sources with the changed-line restriction disabled and with no per-class mutant limit, and MUST report, per class, all mutants of those sources that no test detected, including those on lines changed within the window, so the pre-existing test debt the PR lane no longer measures stays visible and attributable to one run.

Its window base MUST be the last commit before the cutoff on the checked-out branch's first-parent history, so sources merged into that branch within the window remain in scope. Its scope MUST be derived from the repository rather than from a hand-maintained list, by the PR lane's own source and test selection rules applied to the sources the default branch changed within a window the run states in its report. Its time budget MUST be its own; it MUST NOT consume the PR lane's budget, and the PR lane's limits MUST NOT be raised on its behalf.

The scheduled run MUST NOT read the dispositions file for a verdict. It MAY report, as advisory information only, entries that its complete generated set proves stale under CONV-202 rule 5 — a judgement the PR lane's restricted scope cannot make. A run that did not complete MUST report itself incomplete and MUST NOT be read as a complete debt report.

### CONV-301 — Official work budgets

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [README.md](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** Lifecycle refinement: [LifecycleWorkBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/LifecycleWorkBudgetTest.java), [LifecycleLockAllocationBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/LifecycleLockAllocationBudgetTest.java) (local execution). **Planned:** Budget inventory reconciliation against executable work-counter assertions; execute the six mapped suites and the Work budgets block for affected product changes.
- **Evidence class:** doc-only inventory automation; code+local-test lifecycle refinement (not final-head CI acceptance)
- **Gap/note:** The official classification is decided. Command/catalogue reconciliation remains planned follow-up work. Exact six-class inventory is below.

Existing deterministic work-bound assertions, including those outside the current Work budgets command block, MUST be treated as official performance requirements. A bound MUST NOT be widened merely to pass a regression, and a wall-clock assertion MUST NOT replace it. Read, load and query changes MUST run the applicable work budgets and allocation checks. The six classes listed below MUST join the maintained budget inventory and invocation in follow-up tooling work.

Lifecycle budget refinement (Firstmate097, TXN-503/504/802): on FILE_CHANNEL JSON and XML
resources with hashing, path summaries and diff storage disabled, an empty KEEP_OPEN,
KEEP_OPEN_ASYNC_FLUSH or KEEP_OPEN_ASYNC_COMMIT commit (including awaiting its publication)
after loading a flat 16- or 4,096-value document MUST perform 1–2 data-file forces and at most
16 document-record visits, including nested cursor/storage delegations. The allowance is four
root/cursor initialization operations with up to four delegated visits each; it has no term in
document size or retained revisions. The commit MUST publish the next revision and preserve
every value. Closing that clean writer, its session, or its database (with eight additional
readers, while a peer handle/session retains the storage pool) MUST add zero data-file
forces and at most 16 document-record visits per owned writer;
closing it again MUST add zero record visits and forces. All owned readers and writers MUST
be closed; reopening MUST return the committed document. A root traversal of the same
fixture MUST record nonzero work. Cached cursor self-moves and direct document storage
reads MUST each have a nonzero control on both the document counter and the preserved
all-index total, so one connected seam cannot hide a disconnected one.
The document counter excludes name-dictionary and other index-record visits. These bounds
exclude final storage-pool shutdown (which has its own safety flush), optional indexing,
hashing, diff generation and callback delivery,
whose work has separate contracts; they include lifecycle ownership/fencing work.

The uncontended transaction execution lock used by commit/close MUST allocate zero bytes
for acquire/release and close-admission checking after warmup. This allocation check runs in
the existing clean allocation JVM when thread allocation accounting is supported; unsupported
VMs explicitly skip it. Neither budget asserts elapsed time. Verification:
`LifecycleWorkBudgetTest` and `LifecycleLockAllocationBudgetTest`; local execution and
counterfactuals are qualified in [the mutation notes](../docs/lifecycle-mutation-accounting.md).

### CONV-302 — Timed acceptance verdict

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [timed_gate.py](../bundles/sirix-query/bench/common/timed_gate.py) `difference_and_threshold`, `classify`, `stop_states`, `judge`, `allocation_bytes`; [bench_ledger.py](../bundles/sirix-query/bench/common/bench_ledger.py) `stop`, `judge`; [cold-rounds.sh](../bundles/sirix-query/bench/clickbench/cold-rounds.sh) for `SIRIX_BENCH_TIER=gate`; [DEVELOPMENT.md](../docs/DEVELOPMENT.md#the-benchmark-protocol).
- **Verified by:** [test_timed_gate.py](../bundles/sirix-query/bench/common/test_timed_gate.py) `ThresholdTest`, `VerdictTest`, `AAControlTest`, `StopRuleTest`, `BoundaryTest`, `JudgeTest`, `OtherRowsTest`, `AllocationTest`, `ReportTextTest`, `CommandLineTest`; [test_bench_ledger.py](../bundles/sirix-query/bench/common/test_bench_ledger.py) `RunDirectoryTest`; [test_kit_recording.py](../bundles/sirix-query/bench/common/test_kit_recording.py) `ClickBenchColdRoundsTest`.
- **Evidence class:** code+test (local execution, mutation-checked with the survivors recorded in [MUTATION_DISPOSITIONS.md](../bundles/sirix-query/bench/common/MUTATION_DISPOSITIONS.md); the `Benchmark protocol tooling` CI job arrives with the same change and has not reported for this baseline)
- **Gap/note:** Replaces the fixed bar (paired point estimate at most +1%, lower 95% bound at most 0), which failed identical code in about 81% of ten-pair runs of a twelve-benchmark gate (A/A control of 2026-11-23: `main` against itself, 17 accepted pairs, kept as the fixture of `AAControlTest`). Decided by the maintainer on 2026-11-23, with the floor at 4%. No CI job runs a timed comparison: the rule binds whoever drives one, and the analysis refuses a gate run that did not end at its stop point. Of the kits only the ClickBench cold rounds apply the stop rule themselves; other drivers call `bench_ledger.py stop`. Pair admission on outside load is the driver's and is recorded, not checked. A threshold learned from ledger history, as an outlier fence over a benchmark's earlier paired differences, is a later refinement and is not specified here.

**Input.** A timed comparison judges each benchmark its plan names from the accepted pairs of one
run. Pair *i* holds one baseline measurement *aᵢ* and one candidate measurement *bᵢ*, each from its
own fresh process, in the alternating order the plan states. A pair is accepted or rejected by the
plan's admission rule, on evidence of outside load and never on its measurements. A measurement
that is not finite or not greater than zero, a pair that lacks an arm, a pair number that occurs
twice for one benchmark, or two arms that do not hold the same benchmarks MUST make the analysis
fail with a message naming the benchmark and the pair; it MUST NOT produce a verdict from the rest.

**Difference and threshold.** With *n* accepted pairs let *rᵢ* = ln(*bᵢ* / *aᵢ*). The paired
difference MUST be *d* = (1/*n*) Σ *rᵢ*. The noise threshold *T* MUST be computed from the residuals
*eᵢ* = (*rᵢ* − *d*) · √(*n* / (*n* − 1)): for each of the 2ⁿ sign assignments *s* ∈ {−1, +1}ⁿ let
*v*(*s*) = |Σ *sᵢ eᵢ*| / *n*; with the 2ⁿ values in ascending order, *T* is the value at 1-based
rank ⌈0.99 · 2ⁿ⌉. An assignment is one way of swapping the two arm labels within the pairs, so *T*
is the size the paired difference reaches in 1% of relabelings by spread alone. *T* MUST NOT change
when every *bᵢ*, or every *aᵢ*, is multiplied by the same positive constant. Reports state *d* and
*T* in percent as 100 · (eˣ − 1) for a log ratio *x*.

**Verdict.** With the floor *F* = ln 1.04, a benchmark MUST receive exactly one verdict:
`regression` when *d* > *F* and *d* > *T*; `improvement` when −*d* > *F* and −*d* > *T*; otherwise
`unstable` when *T* > *F*; otherwise `unchanged`. A verdict MUST NOT be computed from fewer than 10
or more than 20 accepted pairs.

**Stop rule.** A benchmark has met its stop condition when it has at least *N₀* accepted pairs and
*T* ≤ ln 1.03, or when it has 20 accepted pairs. *N₀* is 10 unless the plan, before the run, states
a larger value of at most 20. A run MUST end at the first point at which every judged benchmark has
met its stop condition; when the plan measures its benchmarks together, a benchmark that has met
the condition keeps running until the last one has, and never past 20 accepted pairs. The decision
to continue or stop MUST depend on pair counts and thresholds only. Verdicts MUST be computed once,
after the run has ended, from all accepted pairs, and a run that ended at any other point MUST NOT
receive one. A pair MUST NOT be added after a verdict has been computed, and a rejected pair MUST
NOT be admitted later. A control run, whose purpose is to measure the gate itself — an A/A run of
one build against itself is one — MAY follow a pair count that its plan fixes in place of the stop
rule; its verdicts are computed the same way and are not acceptance evidence for a change.

**Outcome.** A comparison MUST fail when any judged benchmark's verdict is `regression` or its
allocation check fails. An `unstable` verdict MUST be reported as not judged: it MUST NOT fail the
comparison, and it MUST NOT be reported or counted as showing no regression. It MUST be answered by
an issue against the benchmark, named where the comparison is reported, and MUST NOT be answered by
changing the floor, the stop precision or the pair limits, or by leaving the benchmark out of that
run's report. A plan MAY leave a benchmark out before the run, stating why.

**Allocation.** The allocation rule is unchanged. Allocation per operation MUST be measured for
each arm in at least one fresh process that contributes no timed measurement, so that no timed
process carries the allocation profiler. An arm's figure is the median over its allocation
processes, without rounding. The delta is the candidate's median minus the baseline's median,
rounded to the nearest whole byte, ties to the even byte. The check passes when that delta is at
most zero. If allocation legs were measured for a judged series, each arm MUST retain at least one
accepted allocation leg; otherwise judgment MUST be refused. A series with no allocation legs has
no allocation check. Allocation has no threshold and no `unstable` verdict.

### CONV-303 — Results ledger and conditions record

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [bench_ledger.py](../bundles/sirix-query/bench/common/bench_ledger.py) `capture_environment`, `fingerprint`, `parse_conditions`, `validate_row`, `append_rows`, `begin`, `record`, `commit`; `bench_run_begin` in [bench-common.sh](../bundles/sirix-query/bench/common/bench-common.sh); [cold-rounds.sh](../bundles/sirix-query/bench/clickbench/cold-rounds.sh); [run-benchmark.sh](../bundles/sirix-query/bench/jsonbench/run-benchmark.sh); [CONDITIONS.md](../docs/bench/CONDITIONS.md).
- **Verified by:** [test_bench_ledger.py](../bundles/sirix-query/bench/common/test_bench_ledger.py) `EnvironmentCaptureTest`, `CaptureDetailTest`, `FingerprintTest`, `ConditionsRecordTest`, `ConditionsDetailTest`, `RowSchemaTest`, `RowDetailTest`, `LedgerFileTest`, `LedgerFileDetailTest`, `RunDirectoryTest`, `ImporterTest`, `ImporterDetailTest`, `CommandLineTest`, `CommandLineDetailTest`; [test_kit_recording.py](../bundles/sirix-query/bench/common/test_kit_recording.py) `JsonBenchKitTest`, `ClickBenchColdRoundsTest`.
- **Evidence class:** code+test (local execution, mutation-checked with the survivors recorded in [MUTATION_DISPOSITIONS.md](../bundles/sirix-query/bench/common/MUTATION_DISPOSITIONS.md); the `Benchmark protocol tooling` CI job arrives with the same change and has not reported for this baseline)
- **Gap/note:** The ClickBench cold rounds and the JSONBench runner record by themselves; the JMH module and the bitemporal kit do not, and their drivers call `bench_ledger.py`. The capture reads Linux sources; on another system each probe is recorded as unavailable. A kit does not know the runtime of an arm it launches through Gradle or of an ahead-of-time image and records it as unavailable unless the driver states it. The evidence a `continues` entry names is judged in review, not by the tool. The ledger is local to the measuring machine and is not committed; a pull request cites the rows it relies on.

**Ledger.** Every timed run under the benchmark protocol — a per-change gate, a control such as an
A/A run, a nightly run — MUST append its rows to the results ledger, the file `SIRIX_BENCH_LEDGER`
names, before it reports a verdict and whatever the verdict is; a run that was rejected or aborted
MUST still record the processes it measured. A figure the timer reports as zero is below its
resolution: it is not a measurement and is not recorded, and the kit names it. The ledger is UTF-8
JSON Lines, one object per line.
A writer MUST only append and MUST NOT rewrite or remove a line. Retrying a commit MUST NOT
duplicate a run, measurement or verdict already in the ledger. A matching row identity with
different contents MUST refuse the whole append. A run MUST NOT be rejected after any of its
measurements has reached the ledger, even if its recording marker is absent.
A ClickBench hot figure requires every later try of that process to succeed; a failed (null) try
MUST withhold both that query's hot figure and the suite hot total, without discarding cold figures.
A reader MUST ignore a field it does not know and MUST reject a row whose `schema` it does not know.
Without `SIRIX_BENCH_LEDGER` a kit still runs and still captures its environment, and its numbers
are not protocol results.

Every row carries `schema` (integer, 1), `kind` and `run`. `run` is the run's identifier: its UTC
start as `YYYYMMDDTHHMMSSZ`, a hyphen and 8 lowercase hexadecimal digits.

| `kind` | Field | Type | Content |
|---|---|---|---|
| `run`, one per run, written first | `started_at` | string | UTC start, RFC 3339 |
| | `tier` | string | `gate`, `control` or `nightly` |
| | `night` | string | `nightly` only: the night's UTC date, `YYYY-MM-DD` |
| | `kit` | string | the kit that ran, such as `clickbench-cold`, `jsonbench` or `jmh` |
| | `plan` | object | a paired run: `floor`, `stop_precision`, `quantile`, `min_pairs`, `max_pairs` and `benchmarks`, the names of the judged benchmarks, or null for every benchmark the run measures; a single-arm run: `min_processes` and `benchmarks` |
| | `arms` | object | per arm (`baseline` and `candidate`, or `head`): `commit` (40 hexadecimal digits), `dirty` (boolean), `binary` (`jvm`, `native` or `native-pgo`), `runtime` (the runtime's version line) |
| | `conditions` | string | id of the conditions entry in force |
| | `fingerprint` | string | 64 lowercase hexadecimal digits, defined below |
| | `environment` | object | the environment capture, defined below |
| `measurement`, one per fresh process and metric | `benchmark` | string | name, stable within the kit |
| | `params` | object | string to string; includes dataset identity (`tier` for JSONBench, caller-stated `dataset` for ClickBench cold rounds) |
| | `metric`, `unit` | string | such as `cold_seconds` and `s`, `ns_per_op` and `ns/op`, `alloc_bytes_per_op` and `B/op` |
| | `arm` | string | `baseline`, `candidate` or `head` |
| | `pair`, `position` | integer or null | 1-based pair and the arm's place in it, 1 or 2; null in an unpaired tier |
| | `process` | integer | 1-based ordinal of the fresh process within its arm and benchmark |
| | `value` | number | finite; greater than zero for a time |
| | `samples` | object, optional | `count`, `min`, `max` when the value summarises several tries of one process |
| | `counters` | object, optional | the route counters the kit printed, name to number |
| | `accepted` | boolean | false when the admission rule rejected the pair or process, or the run as a whole was rejected, for instance because its answers were wrong |
| | `reject_reason` | string | present exactly when `accepted` is false |
| `verdict`, one per judged benchmark of a `gate` or `control` run | `benchmark`, `params`, `metric`, `unit` | | as above |
| | `pairs` | integer | accepted pairs the verdict used |
| | `difference`, `threshold` | number | *d* and *T* of CONV-302, as natural-log ratios |
| | `verdict` | string | `regression`, `improvement`, `unstable` or `unchanged` |
| | `allocation` | object or null | `baseline_bytes`, `candidate_bytes` (whole bytes for display), `delta_bytes` (the rounded median delta) and `pass` (boolean, delta at most zero); null when the plan measures no allocation |

**Environment capture.** Before its first timed process a run MUST write `environment.json` into
its output directory and embed the same object in its `run` row. The capture MUST hold: the capture
time; kernel release; operating-system name and version; machine architecture; firmware vendor,
version and date and the product name; CPU model, sockets, cores, threads and the `lscpu` output;
for every cpufreq policy its driver, governor, minimum and maximum frequency and
energy-performance preference; the turbo or boost setting; the powercap limits; the
`cpupower frequency-info` output; the `sensors` output; total memory; the CPUs the run is allowed
to use; the version output of every JDK or GraalVM an arm runs on; and the type of the filesystem
that holds the benchmark data. A probe whose source is missing or unreadable MUST be recorded as
unavailable with the reason; it MUST NOT be omitted, guessed or copied from an earlier run. The
capture MUST NOT hold a host name, a user name, a serial number or a path below a home directory,
because conditions are published.

**Fingerprint.** The fingerprint is the SHA-256, in lowercase hexadecimal, of the canonical JSON
(UTF-8, keys sorted, no insignificant whitespace) of an object holding exactly: kernel release,
operating-system name and version, architecture, firmware version, product name, CPU model,
logical CPU count, the sorted set of cpufreq drivers, the sorted set of governors, the turbo or
boost setting and total memory in whole GiB. Readings that vary within one condition —
temperatures, current frequencies, powercap limits the platform moves by itself, times — are
captured and not fingerprinted. Runtimes are recorded per arm and are not fingerprinted, because a
comparison may set two runtimes against each other.

**Conditions record.** `docs/bench/CONDITIONS.md` MUST hold one dated entry for every change of
the measurement environment: machine, firmware, kernel or operating system, frequency policy,
power cap, default runtime, load control, or the benchmark set. Entries are appended in date order;
an entry MUST NOT be edited afterwards except by a dated correction. An entry's heading is
`## YYYY-MM-DD — <id> (new baseline)` or `## YYYY-MM-DD — <id> (continues)`, where `<id>` consists
of lowercase letters, digits and hyphens, and its body is a table with the rows Machine, CPU,
Memory, Storage, Firmware, Kernel and OS, Frequency policy, Runtimes, Load control, Benchmark set,
Changed, Comparable with and Fingerprint. A `new baseline` entry starts a condition under a new
id, even when the fingerprint is unchanged. A `continues` entry MUST refer to an id started by an
earlier entry and keeps that id; its Changed row MUST say what changed, and its Comparable with row
MUST name the control run in the ledger that shows results stayed comparable. A run that writes to
the ledger MUST find the latest entry containing its fingerprint and record that entry's id as its
`conditions`; when no entry holds the fingerprint, the run MUST refuse to start its timed processes,
naming the fingerprint and the fields that differ from the
previous run in the ledger. Rows that carry different conditions ids MUST NOT be compared with
each other, in a trend window or in a learned threshold.

### CONV-304 — Nightly trend tier

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [bench_trend.py](../bundles/sirix-query/bench/common/bench_trend.py) `nightly_series`, `state_of`, `report`; [DEVELOPMENT.md](../docs/DEVELOPMENT.md#the-benchmark-protocol).
- **Verified by:** [test_bench_trend.py](../bundles/sirix-query/bench/common/test_bench_trend.py) `QuartileTest`, `StateTest`, `SeriesTest`, `ReportTest`.
- **Evidence class:** code+test (local execution on constructed histories, mutation-checked with the survivors recorded in [MUTATION_DISPOSITIONS.md](../bundles/sirix-query/bench/common/MUTATION_DISPOSITIONS.md); the `Benchmark protocol tooling` CI job arrives with the same change and has not reported for this baseline)
- **Gap/note:** Only the analysis exists. No machine runs a nightly, so no night has been recorded and the constants below have not met real nightly data; they are to be re-examined, in a change of their own, once the dedicated machine has recorded its first window. Scheduling is follow-up work.

**Run.** A nightly run measures one arm, `head`: the default branch's build at that night's commit,
with at least 20 accepted fresh processes for every benchmark, under the machine rules of the
benchmark protocol. It records tier `nightly` and its `night` in the ledger (CONV-303). A night
has at most one run with accepted measurements for a condition.

**Series and level.** A series is the set of accepted measurements that share conditions id, kit,
benchmark, params, metric, binary kind and runtime. A night's level *L* in a series is the median
of that night's accepted values. A night with fewer than 20 of them is not a recorded night of that
series, and two runs with accepted measurements for one night and series MUST make the analysis
fail, naming both.

**Drift.** For night *t* with at least 14 earlier recorded nights, the window is the 14 most recent
of them. The baseline *B* is the median level of the 7 oldest nights of the window, and the drift
is *D* = ln(*Lₜ* / *B*). The series' own noise is taken from the window: with *cₖ* the absolute log
ratio of the levels of two adjacent nights of the window (13 values) and *Q₁*, *Q₃* their quartiles
by linear interpolation between order statistics, the fence is *Φ* = *Q₃* + 1.5 · (*Q₃* − *Q₁*). With
the bound *G* = ln 1.02, night *t* exceeds upward when *D* > *G* and *D* > *Φ*, and downward when
−*D* > *G* and −*D* > *Φ*.

**State.** Every series MUST receive exactly one state per night: `insufficient-history` with fewer
than 14 earlier recorded nights; `drift` when the night exceeds upward and the previous recorded
night, judged on its own window, exceeded upward too; `unconfirmed` when the night exceeds upward
and the previous one did not; `faster` and `faster-unconfirmed` for the same two cases downward;
otherwise `unstable` when *Φ* > *G*; otherwise `steady`. A change is therefore unconfirmed until the
next night repeats it.

**Report.** For every series whose state is not `steady` the report MUST give the state, *D* and
*Φ* in percent, the night's commit and, for `drift` and `faster`, the night and commit before the
first night of the unbroken run of exceeding nights. The tier MUST NOT pass or fail a change.
`drift` MUST be answered by an investigation recorded in an issue; `unstable` obliges the fixture
work CONV-302 requires and MUST NOT be answered by changing *G*, the window or the fence.

## Status and terminology (CONV-101, CONV-102)

| Status | Meaning |
|---|---|
| enforced | implemented, mapped deterministic default-lane test passed for the assessed baseline |
| partial | some behavior or evidence exists; coverage, lane or execution evidence is incomplete |
| gap | implemented behavior with no adequate mapped behavioral test |
| designed | decided behavior with an implementation mismatch or in-flight implementation |
| proposed | specified follow-up capability not implemented here |
| open | intent or contract detail awaits a decision; alternatives are recorded explicitly |

Evidence classes are `code+test`, `code-only`, and `doc-only (unverified)`.
Source inspection establishes what code and assertions say; it does not establish a test pass.
`implementation-defined` means documented in CONF; `unspecified` promises no behavior.
`Draft` means awaiting chapter adoption; `Adopted` means binding; `Frozen` adds compatibility protection.
Paths are repository-relative, with symbols rather than drifting line numbers.

Trace rollout: report-only before 1.0; then unknown IDs, stale mappings, ID hygiene and dead
citations become blocking, and unmapped MUSTs block new requirements. Passing-result evidence,
test weakening and code/spec co-change checks become blocking only after annotation coverage.
The future tool and its waivers are not present merely because this text describes them.

## Official budget inventory (CONV-301)

These six production-work assertions are official budgets despite not matching the current
Work budgets selectors (the probe infrastructure self-test is not counted).
Each row refines CONV-301 and maps directly to executable assertions; bounds apply to
that suite's stated fixture, not to arbitrary datasets.

| Suite | Independent work bound / observable failure |
|---|---|
| [WriterCatalogueHandoffTest](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/WriterCatalogueHandoffTest.java):assertSuccessorWork | successor writes 1 catalogue only for changed membership, otherwise 0; 0 catalogue listings; 1–2 data forces; clean successor close adds 0 forces |
| [ProjectionOpenRowGroupTailTest](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionOpenRowGroupTailTest.java):memoHitsSkipBasePayloadsAndColdRoutesSkipDictionaryHashes | masked directory defers exactly 1 open tail and reads 0 tail bodies; ordinary BODY chain stays one batch with groups−1 members and 0 point reads |
| [RecordKeySetPredicateTest](../bundles/sirix-core/src/test/java/io/sirix/index/projection/RecordKeySetPredicateTest.java):denseSourceMapsOnceAcrossAThousandLeaves | 1,000,000 rows across 1,000 leaves require at most 1,001,000 key-set advances, with every member retained |
| [TemporalAggregateBoundPresenceTest](../bundles/sirix-query/src/test/java/io/sirix/query/TemporalAggregateBoundPresenceTest.java) | exactly one live group head per binding, 0 projection BODY segments, 0 valid-time interval references, same result as the independent query oracle |
| [BitemporalTemporalAggregateTimestampTest](../bundles/sirix-query/src/test/java/io/sirix/query/bench/bitemporal/BitemporalTemporalAggregateTimestampTest.java) | Q6/Q7/Q8/Q11 decode exactly two declared group heads per binding, 0 projection BODY segments, 0 valid-time interval references, oracle parity |
| [ValidTimeContainerMoveQueryTest](../bundles/sirix-query/src/test/java/io/sirix/query/function/jn/temporal/ValidTimeContainerMoveQueryTest.java):assertArrayScope | 0 posting-reference enumeration; empty membership uses 1 lookup and 0–1 evidence visits, present membership 2 lookups and 1–2 visits, exact expected node keys |

Planned tooling adds these exact selectors to the maintained invocation/catalogue and
checks drift. Existing budgets remain official as well; no bound is weakened here.

One further budget is decided but not yet executable: TXN-311's conditional write check is bounded
by its target, so `ConditionalWritePreconditionBudgetTest` must assert record-read counters of O(1)
for a record-sized target and for an insertion point, and at most the head-revision subtree size for
a subtree target, with no term in resource size, revision count or token age. It joins this inventory when it exists; naming it here does not make it
a passing assertion.

## Deferred work

No new choice is needed to preserve the adopted process. Tool delivery, annotation migration
and a weekly/RC crash lane are required follow-up work, not completed by Markdown.

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| CONV-101 | No existing test mapped. **Planned:** SpecTraceContractTest: parse chapters; reject duplicate/malformed/retired IDs, missing metadata and dangling citations. | proposed |
| CONV-102 | No existing test mapped. **Planned:** SpecTraceEvidenceTest: reject an enforced mapping with no passing JUnit result; accept it only with a default-lane result for the same baseline. | proposed |
| CONV-103 | No existing test mapped. **Planned:** Review acceptance: requirement-to-test-to-change mapping, PIT artifact with survivor dispositions; for docs-only changes, links, IDs, traceability and evidence audit instead of artificial application tests. | proposed |
| CONV-104 | No existing test mapped. **Planned:** SpecTraceContractTest plus fixtures for each failure, annotation discovery, default-lane versus opt-in evidence and PIT selection by requirement. | proposed |
| CONV-105 | No existing test mapped. **Planned:** SpecChangeCompatibilityTest and release review: compare the frozen requirement corpus and API/format baselines, reject an unclassified incompatible change. | proposed |
| CONV-201 | [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `DispositionRecordTest`. | partial |
| CONV-202 | [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `VerdictTest`, `MergeBaseFileTest`, `CensusVerdictTest`, `PartialResultTest`, `ClassReaderTest`. | partial |
| CONV-203 | [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `ReviewTest`, `RefactorHelperTest`, `CounterfactualTest`; review acceptance; sample-refactor churn in the PR. | partial |
| CONV-204 | [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `AssociationTest`; release-contract selection tests in test_pr_mutation.py. | partial |
| CONV-205 | [ChangedLineInterceptorTest.java](../tools/pit-changed-lines/src/test/java/io/sirix/pit/ChangedLineInterceptorTest.java); [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `CensusVerdictTest`; [test_pr_mutation.py](../.github/scripts/tests/test_pr_mutation.py) command construction. | partial |
| CONV-206 | [test_pr_mutation.py](../.github/scripts/tests/test_pr_mutation.py) `FullScopeTest`; workflow review acceptance. | partial |
| CONV-301 | LifecycleWorkBudgetTest and LifecycleLockAllocationBudgetTest locally verify the lifecycle refinement. **Planned:** Budget inventory reconciliation against executable work-counter assertions; execute the six mapped suites and the Work budgets block for affected product changes. | proposed |
| CONV-302 | [test_timed_gate.py](../bundles/sirix-query/bench/common/test_timed_gate.py): pinned thresholds, scale invariance, the 2026-11-23 A/A control, shifted arms, the stop rule, allocation and the error cases; [test_kit_recording.py](../bundles/sirix-query/bench/common/test_kit_recording.py) `ClickBenchColdRoundsTest`. | partial |
| CONV-303 | [test_bench_ledger.py](../bundles/sirix-query/bench/common/test_bench_ledger.py): capture, fingerprint, conditions record, row schema, append-only file, run directory, importers; [test_kit_recording.py](../bundles/sirix-query/bench/common/test_kit_recording.py). | partial |
| CONV-304 | [test_bench_trend.py](../bundles/sirix-query/bench/common/test_bench_trend.py): steady, step, slow drift, unconfirmed outlier, speedup, unstable series, short history, conditions boundary, report. | partial |
