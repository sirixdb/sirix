# CONV — Conventions and conformance

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 document conventions, traceability rollout, mutation dispositions and test associations for the PR mutation lane (CONV-201 to CONV-204) and the accepted work-budget classification. The mutation requirements are implemented in the CI reporter; the proposed trace tool remains follow-up work. Code fields for process requirements point to the authoritative workflow artifacts; no product method enforces them.

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
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) `judge_entry`, `judge_dispositions`, `apply_dispositions`, `is_clean`, `read_class`; [gradle.yml](../.github/workflows/gradle.yml)
- **Verified by:** [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `VerdictTest` (end-to-end `run` over fixture reports, class images and files: both kinds reported apart, NO_COVERAGE and other statuses never dispositioned, line-shifted and near-miss identities, unconfirmed/contradicted/superseded/stale/not-judged/inactive entries, re-confirmation by changed method only, invalid files, incomplete runs) and `MergeBaseFileTest`; `ClassReaderTest`; reader differential against `javap` on 300 compiled core classes recorded in the PR.
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** Extends the existing [blocking criterion](../docs/VERIFICATION.md#blocking-criterion-the-prs-own-lines), whose changed-line attribution, incomplete-analysis rules and use of PIT's `detected` attribute (`TIMED_OUT`, `MEMORY_ERROR` and `RUN_ERROR` count as detected) are unchanged. Applies to the PR lane only; the weekly curated core PIT task reads no dispositions. Under per-class sampling an absent mutant cannot be told from an unsampled one, so mutant-level staleness is judged only where the sample is provably complete. A reported count below the limit is no such proof: PIT 1.25.5 runs its class limit (an exactly-`limit` stride sample, one entry per binary class) before later equivalence filters such as `EquivalentReturnMutationFilter`, which can remove sampled mutants. In the sampled lane, re-confirmation by changed method (rule 2) catches most identity-changing edits; the local check judges complete samples from a focused run without a limit.

Tests MUST NOT be written to cover lines or to kill mutants for their own sake. A surviving or uncovered mutant is a signal about the specification and its behavior tests, resolved as CONV-103 lists. In particular a changed line that no selected test executes (`NO_COVERAGE`) MUST be resolved by exactly one of: (1) the specified behavior that needs this code receives a behavior test derived from its requirement, not aimed at the line, which then covers it; (2) the code has no specified behavior and is removed; (3) existing behavior tests do cover it but the lane did not select them, and a CONV-204 association makes the lane run them. A `NO_COVERAGE` mutant MUST NOT be dispositioned by either kind, because a mutant that never executed has shown nothing.

Definitions for one lane run. A *changed-line mutant* is a reported mutant attributed to a line the PR changed, as defined by the existing blocking criterion. A method is *changed* when a line of its compiled line-number table is a changed line of its source. An entry is *added* when the dispositions file at the merge base of the PR's base and checked-out revisions contains no entry equal to it in every field, and *pre-existing* otherwise; an added entry is a *re-confirmation* when the merge-base file holds an entry with the same identity, and *new* otherwise. If the merge-base file is absent or invalid, every entry MUST be treated as new. A *mutated class* is a class in the `targetClasses` of a module whose run completed. A mutated class's sample is *complete* only when the run applied no per-class mutant limit.

The run MUST be clean only if the existing blocking criterion's completeness conditions hold, the dispositions file is valid (CONV-201), and every one of the following holds:

1. Every changed-line mutant is detected, or has status `SURVIVED` and is matched by an added entry. A changed-line `NO_COVERAGE` mutant MUST fail even when matched, and so MUST every other undetected status.
2. No pre-existing entry belongs to a changed method. Such an entry MUST fail as *needs re-confirmation*: the author re-reviews the argument (and, for `killed-out-of-lane`, re-runs the counterfactual) and updates the entry, at least its `checked_at`. Entries of methods the PR did not change MUST NOT need re-confirmation, whatever else changed in their file.
3. Every new entry matches a `SURVIVED` mutant reported in this run. A new entry that matches nothing, and any added entry that matches an undetected mutant whose status is not `SURVIVED`, MUST fail as *unconfirmed*; matches to detected mutants follow rule 4. Since only changed sources are mutated, entries cannot be added for code outside the PR's changed files; a new entry may match a `SURVIVED` mutant on an unchanged line of a changed file, which lets an author re-identify an entry after a refactor (CONV-203), and the summary MUST mark such matches as on an unchanged line.
4. No `equivalent` entry matches a `KILLED` mutant: it MUST fail as *contradicted*, because a detecting test refutes the claim. No `killed-out-of-lane` entry matches a detected mutant: it MUST fail as *superseded*, because an in-lane test now detects it. An `equivalent` entry matching `TIMED_OUT`, `MEMORY_ERROR` or `RUN_ERROR` MUST be reported, not failed, because those statuses can arise from timing or infrastructure.
5. No entry is *stale*. An entry MUST be stale when its class is absent from every module's compiled main classes, its method name and descriptor are absent from that class, or, for `killed-out-of-lane`, its test class is no longer declared in any module's test sources, all checked for every entry in every run; or when its class is a mutated class with a complete sample and no reported mutant matches it.

A re-confirmation that matches no reported mutant in a class whose sample is incomplete or that this run did not mutate MUST be reported as *not judged*, without failing, and so MUST a pre-existing entry in a class whose sample is incomplete. A pre-existing entry of an unchanged method that matches a mutant with a non-dispositionable undetected status MUST be reported as *inactive*, without failing. Pre-existing entries of unchanged methods that match `SURVIVED` mutants on unchanged lines MUST be reported as advisory dispositions and MUST NOT change the outcome.

The job summary and `results.json` MUST keep the two kinds apart and list, per module: the changed-line mutants killed, otherwise detected, dispositioned as equivalent, dispositioned as killed out of lane (with the named test) and failing; each dispositioned mutant with its matching entry's `checked_at` and justification (the summary MAY truncate the justification; `results.json` MUST NOT); each failing mutant with its reason (no entry, not dispositionable status); and every entry in a failing or reported state (needs re-confirmation, unconfirmed, contradicted, superseded, stale, not judged, detected-other) with its identity, its position in the file and the action that resolves it, naming the local helper command for stale and re-confirmation states. Validation errors MUST name the entry's position in the file and the violated rule. All file-derived text MUST be escaped as existing summary text is.

### CONV-203 — Disposition review, abuse limits and refactoring

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) `format_dispositions`, `build_parser`, `check_dispositions`, `remap_dispositions`, `counterfactual`; [VERIFICATION.md](../docs/VERIFICATION.md#reviewed-dispositions)
- **Verified by:** [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `ReviewTest` (summary counts and full added claims, escaping, CI commands without disposition input, removal monotone over every subset), `RefactorHelperTest`, `CounterfactualTest`. Review acceptance for the reviewer obligation; the PR records disposition churn measured on a sample refactor.
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** Precedents compared: PIT `excludedMethods`/`excludedClasses`/`avoidCallsTo` and its `@Generated`/`@DoNotMutate` filter remove whole methods or classes before mutation, invisibly in results; Stryker's `// Stryker disable next-line <mutators>: <reason>` comments keep an optional reason in the diff but cover a source line and a mutator list, not one mutant; mutmut's and cosmic-ray's `# pragma: no mutate` cover whole lines, and cosmic-ray's operator filter covers operators by regular expression. This requirement keeps their visibility in review and rejects their breadth: exact per-mutant identity, a mandatory reason, re-confirmation when the method changes, and no numeric cap or label, for the reasons stated in the requirement.

Dispositions are expected to be rare: behavior, budget and latency tests detect most mutants, and an entry records a reviewed exception, not a way to finish a PR. A disposition MUST apply to one mutant identity; class-, method-, mutator-, line- or file-wide waivers MUST NOT exist in the file, the reporter or the workflow. Dispositions MUST come only from the dispositions file of the checked-out revision and of the merge base; the reporter MUST NOT accept dispositions or a relaxation of CONV-202 from command-line flags, environment variables, PR labels, comments or artifacts. The job summary MUST open its disposition section with the number of added and removed entries and MUST list every added entry with its kind, identity and full justification, so a reviewer sees each claim beside the diff. A reviewer approving a PR that adds or changes entries MUST check each justification against CONV-201's standard. No per-PR numeric cap and no approval label applies: a cap would push authors toward line-targeted tests that CONV-202 forbids, and a label would need a re-run on every label event. Removing an entry MUST always be permitted; it can only withdraw dispositions and MUST NOT turn any undetected changed-line mutant from failing into accepted.

A pure refactor MUST stay cheap. The reporter MUST offer local helpers that CI never runs: a read-only *check* that, given a PIT report and the compiled classes, lists stale, re-confirmation, unconfirmed and superseded entries with the action for each; a read-only *remap* that, for each stale entry, proposes candidate identities from the report with the same mutator and description, preferring the same method with other instruction indexes, then a renamed method of the same class, then a method of the same name and descriptor in another class, and prints every eligible candidate once in tier order, labelling each with its tier and whether its instruction indexes match, for the author to review without writing the file; and a *counterfactual* runner that hashes the touched sources, checks that the test passes on them, applies the injection patch, runs the test in its own process, reverses the patch, verifies the hashes and prints the `evidence` table, refusing to print one when the baseline fails, the injected run passes or a hash differs. No helper output MUST be applied to the dispositions file automatically.

### CONV-204 — Lane test associations

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) `parse_associations`, `load_associations`, `test_scope`; [mutation-test-associations.toml](../.github/mutation-test-associations.toml)
- **Verified by:** [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `AssociationTest`; [test_pr_mutation.py](../.github/scripts/tests/test_pr_mutation.py) release-contract selection tests read the repository file.
- **Evidence class:** code+test (mapped tests not yet passed in a default CI lane for this baseline)
- **Gap/note:** Generalizes the reporter's hard-coded `CONTRACT_TESTS`, which the file replaces. Selecting tests by measured coverage of the changed classes would remove the need for associations but needs a per-test coverage map inside the lane's budget (issue 1411); it is recorded follow-up work. A test excluded from PIT's baseline by the Gradle configuration (isolated allocation suites, soak and random property tests) does not run even when associated; its effects are resolved through CONV-201's `killed-out-of-lane` kind.

The repository file `.github/mutation-test-associations.toml` MUST state which existing behavior tests the lane must run for a changed class, and nothing more. Its only top-level key MUST be `association`, an array of tables with exactly the fields `class`, the binary name of a top-level main class declared by a source the lane can select, and `tests`, a non-empty list of binary names of top-level test classes declared in the same module. Names MUST be exact; a pattern character, an unknown or missing field, a duplicate `class`, a class with no declaring main source, or a test not declared in that module MUST make the file invalid, and an invalid file MUST make the run not clean with a message naming the entry and the rule. Ownership MUST follow the declared package and top-level Java/Kotlin types, including Kotlin file facades for top-level functions or properties (the default `<File>Kt`, or the file's `JvmName`), independently of source filenames and directories. Class literals and other member references MUST NOT count as declarations or hide subsequent declarations. When a source is selected, the tests associated with every top-level binary class it declares MUST be added to its candidate tests and mark it resolved even if no name-based rule matched; they MUST count toward the module test cap as other candidates do.

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
| CONV-202 | [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `VerdictTest`, `MergeBaseFileTest`, `ClassReaderTest`. | partial |
| CONV-203 | [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `ReviewTest`, `RefactorHelperTest`, `CounterfactualTest`; review acceptance; sample-refactor churn in the PR. | partial |
| CONV-204 | [test_pr_mutation_dispositions.py](../.github/scripts/tests/test_pr_mutation_dispositions.py) `AssociationTest`; release-contract selection tests in test_pr_mutation.py. | partial |
| CONV-301 | LifecycleWorkBudgetTest and LifecycleLockAllocationBudgetTest locally verify the lifecycle refinement. **Planned:** Budget inventory reconciliation against executable work-counter assertions; execute the six mapped suites and the Work budgets block for affected product changes. | proposed |
