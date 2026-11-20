# CONV — Conventions and conformance

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 document conventions, traceability rollout, mutation dispositions for the PR mutation lane (CONV-201 to CONV-203) and the accepted work-budget classification. This does not implement the proposed trace tool or change CI. Code fields for process requirements point to the authoritative workflow artifacts; no product method enforces them.

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

A feature or behavioral fix MUST state behavior, invariants and errors and derive tests from those requirements before product implementation. Each test oracle MUST distinguish at least one plausible violation independently of implementation details. Changed logic MUST receive mutation testing. Each surviving or uncovered mutant MUST be resolved in exactly one of three ways: (1) a behavior test, derived from the requirements, that detects it through observable behavior; (2) an equivalence disposition under CONV-201 to CONV-203; or (3) removal of the mutated code, when the mutant shows that code has no observable effect. A test that detects a mutant only by inspecting private state, internal call counts or other implementation details that no requirement specifies MUST NOT count as resolution (1). Bug fixes MUST include the postmortem template, regression and lesson required by CONTRIBUTING.

### CONV-104 — Traceability rollout

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [CONTRIBUTING.md](../CONTRIBUTING.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** SpecTraceContractTest plus fixtures for each failure, annotation discovery, default-lane versus opt-in evidence and PIT selection by requirement.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** spec_trace.py, @Verifies, CI wiring and test annotations are intentionally planned: this task changes documentation only.

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
- **Status:** proposed
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) (planned); `.github/mutation-dispositions.toml` (planned)
- **Verified by:** No existing test mapped. **Planned:** `test_pr_mutation.py` disposition-file fixtures written from this requirement: a valid entry; each missing, unknown, empty or mistyped field; a glob or pattern character in every identity field; a duplicate identity; a classification other than `equivalent`; a short or placeholder justification; a malformed revision.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** Today the PR lane has no disposition input: only resolutions (1) and (3) of CONV-103 can make it clean. The identity fields are PIT 1.25.5 `mutations.xml` elements; across 170 saved Sirix PIT reports (40,054 mutants) the six-field identity had no collision, dropping the instruction indexes caused 23,853 collisions and replacing them by the line number 4,640, and 399 of 512 identities shared by two revisions of one change kept their identity while their line moved.

An equivalence disposition MUST be an entry in the repository file `.github/mutation-dispositions.toml`, a TOML document whose only top-level key is `mutant`, an array of tables; an absent file MUST mean no entries. Each entry MUST have exactly the fields below, each non-empty; an unknown, missing, empty or mistyped field MUST make the whole file invalid.

| Field | Content | Role |
|---|---|---|
| `class` | `<mutatedClass>`: the JVM binary name, e.g. `io.sirix.Foo$Bar` | identity |
| `method` | `<mutatedMethod>`, e.g. `put`, `<init>`, `lambda$put$0` | identity |
| `descriptor` | `<methodDescription>`: the JVM method descriptor | identity |
| `mutator` | `<mutator>`: PIT's fully qualified mutator id | identity |
| `indexes` | the integers of `<indexes>`, in report order: PIT's instruction ordinals within the method | identity |
| `description` | `<description>`, compared as exact text | identity |
| `classification` | the string `equivalent` | review |
| `checked_at` | full 40- or 64-hex commit ID whose PIT report the author inspected | review |
| `justification` | the equivalence argument | review |

The six identity fields form the mutant's identity, PIT's own mutation identifier (class, method, descriptor, instruction indexes, mutator) plus its description. An entry MUST match a mutant only when all six are equal as exact strings and integer lists. Fields MUST NOT be interpreted as patterns. A value not well formed for its field MUST make the file invalid: `class` and `mutator` MUST be dot-separated Java identifiers (`$` allowed), `method` a JVM unqualified method name (`<init>`, `<clinit>` or a name without `.`, `;`, `[`, `/`, `<`, `>`), `descriptor` a JVM method descriptor, and `indexes` non-negative integers; a glob, regular-expression or wildcard character (`*`, `?`, `[`, `]`, `{`, `}`, `|`, `\`) MUST make the file invalid in every identity field except where the descriptor grammar requires `[` for an array type. Line numbers MUST NOT be part of the identity, so an entry survives edits that only move the mutant's line; an edit of the same method that changes the mutant's instruction ordinal or description deliberately makes the entry stale (CONV-202). Two entries with the same identity MUST make the file invalid.

`classification` MUST be `equivalent`; no other value, including "hard to test", "slow", "flaky", "covered elsewhere" or "pre-existing", is a disposition. `justification` MUST argue, from the specification, the public contract or a documented invariant, that no observable behavior differs between the original and the mutated program for any input the contract admits; observable behavior includes results, errors, persisted state and the bounds of official work budgets (CONV-301). It MUST NOT argue from test difficulty or test selection. The reporter MUST reject a justification shorter than 60 characters after whitespace normalization or containing, case-insensitively, `hard to test`, `difficult to test`, `untestable`, `not testable`, `TODO`, `TBD` or `FIXME`; these checks are a floor, and review (CONV-203) judges the argument. A mutant that shows the mutated code has no observable effect at all MUST be resolved by removing that code (CONV-103 resolution (3)), not by an entry. `checked_at` records evidence provenance only; the reporter MUST validate its form, not its ancestry, because merge strategies may make it unreachable.

### CONV-202 — PR mutation verdict with dispositions

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) (planned); [gradle.yml](../.github/workflows/gradle.yml)
- **Verified by:** No existing test mapped. **Planned:** `test_pr_mutation.py` CLI tests over fixture `mutations.xml` files and disposition files written from this requirement, observing exit status, `results.json` and `summary.md`: all changed-line mutants killed; a changed-line `SURVIVED` and a `NO_COVERAGE` mutant with and without an added entry; an entry matching a line-shifted mutant; an added entry on an unchanged line, on an unmutated class and on a `KILLED` mutant; a pre-existing entry matched on a changed line; stale entries by absent class, absent method and, under an unbound sample, absent mutant; an invalid file; incomplete runs with valid entries.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** Extends the existing [blocking criterion](../docs/VERIFICATION.md#blocking-criterion-the-prs-own-lines), whose changed-line attribution, incomplete-analysis rules and use of PIT's `detected` attribute (`TIMED_OUT`, `MEMORY_ERROR` and `RUN_ERROR` count as detected) are unchanged. Applies to the PR lane only; the weekly curated core PIT task reads no dispositions. Under per-class sampling an absent mutant cannot be told from an unsampled one, so mutant-level staleness is judged only where the sample is provably complete.

Definitions for one lane run. A *changed-line mutant* is a reported mutant attributed to a line the PR changed, as defined by the existing blocking criterion. An entry is *added* when the dispositions file at the merge base of the PR's base and checked-out revisions contains no entry equal to it in all nine fields; an entry is *pre-existing* otherwise. If the merge-base file is absent or invalid, every entry MUST be treated as added. A *mutated class* is a class in the `targetClasses` of a module whose run completed. A mutated class's sample is *complete* when the report holds fewer of its mutants than that module's per-class limit, or when no limit was applied.

The run MUST be clean only if the existing blocking criterion's completeness conditions hold, the dispositions file is valid (CONV-201), and every one of the following holds:

1. Every changed-line mutant is detected, or has status `SURVIVED` or `NO_COVERAGE` and is matched by an **added** entry. A changed-line mutant matched only by a pre-existing entry MUST fail as *needs re-review*: a change to a line carrying an equivalence argument MUST re-confirm it, which the author does by re-reviewing the justification and updating `checked_at`. Undetected statuses other than `SURVIVED` and `NO_COVERAGE` MUST fail even when matched.
2. Every added entry matches a changed-line mutant of this run whose status is not `KILLED`. An added entry that matches nothing, or matches only a mutant on an unchanged line, MUST fail as *unconfirmed*; entries therefore cannot be added for code the PR did not change.
3. No entry matches a `KILLED` mutant. Such an entry MUST fail as *contradicted*: a detecting test refutes the equivalence claim, and the entry MUST be removed. A match to another detected status (`TIMED_OUT`, `MEMORY_ERROR`, `RUN_ERROR`) MUST be reported, not failed, because those statuses can arise from timing or infrastructure.
4. No entry is *stale*. An entry MUST be stale when its class is absent from every module's compiled main classes, or its method name and descriptor are absent from that class, checked for every entry in every run; or when its class is a mutated class with a complete sample and no reported mutant matches it.

An entry whose class is mutated but whose sample is incomplete and which matches no reported mutant MUST be reported as *not judged*, without failing. Pre-existing entries matching undetected mutants on unchanged lines MUST be reported as advisory dispositions and MUST NOT change the outcome; they reduce the advisory debt shown for those classes.

The job summary and `results.json` MUST list, per module: the changed-line mutants killed, otherwise detected, dispositioned and failing; each dispositioned mutant with its matching entry's `checked_at` and justification (the summary MAY truncate the justification; `results.json` MUST NOT); each failing mutant with its reason (no entry, needs re-review, status not dispositionable); and every entry in a failing or reported state (unconfirmed, contradicted, stale, not judged, detected-other) with its identity. Validation errors MUST name the entry's position in the file and the violated rule. All file-derived text MUST be escaped as existing summary text is.

### CONV-203 — Disposition review and abuse limits

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [pr_mutation.py](../.github/scripts/pr_mutation.py) (planned); [gradle.yml](../.github/workflows/gradle.yml); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** `test_pr_mutation.py`: the summary's added-entry section lists every added entry with its full justification; the CI invocation carries no flag, environment variable or input that adds, ignores or relaxes dispositions; removing an entry never turns a failing changed-line mutant into an accepted one. Review acceptance for the reviewer obligation.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** Precedents compared: PIT `excludedMethods`/`excludedClasses`/`avoidCallsTo` and its `@Generated`/`@DoNotMutate` filter remove whole methods or classes before mutation, invisibly in results; Stryker's `// Stryker disable next-line <mutators>: <reason>` comments keep an optional reason in the diff but cover a source line and a mutator list, not one mutant; mutmut's and cosmic-ray's `# pragma: no mutate` cover whole lines, and cosmic-ray's operator filter covers operators by regular expression. This requirement keeps their visibility in review and rejects their breadth: exact per-mutant identity, a mandatory reason, re-confirmation on change, and no numeric cap or label, for the reasons stated in the requirement.

A disposition MUST apply to one mutant identity; class-, method-, mutator-, line- or file-wide waivers MUST NOT exist in the file, the reporter or the workflow. Dispositions MUST come only from the dispositions file of the checked-out revision and of the merge base; the reporter MUST NOT accept dispositions or a relaxation of CONV-202 from command-line flags, environment variables, PR labels, comments or artifacts. The job summary MUST open its disposition section with the number of added and removed entries and MUST list every added entry with its identity and full justification, so a reviewer sees each claim beside the diff. A reviewer approving a PR that adds or changes entries MUST check each justification against CONV-201's standard. No per-PR numeric cap and no approval label applies: a cap would push authors toward internal-peeking tests that CONV-103 forbids, and a label would need a re-run on every label event. Removing an entry MUST always be permitted; it can only withdraw dispositions and MUST NOT turn any undetected changed-line mutant from failing into accepted.

### CONV-301 — Official work budgets

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [README.md](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** Budget inventory reconciliation against executable work-counter assertions; execute the six mapped suites and the Work budgets block for affected product changes.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** The official classification is decided. Command/catalogue reconciliation is planned, not performed in this spec-only task. Exact six-class inventory is below.

Existing deterministic work-bound assertions, including those outside the current Work budgets command block, MUST be treated as official performance requirements. A bound MUST NOT be widened merely to pass a regression, and a wall-clock assertion MUST NOT replace it. Read, load and query changes MUST run the applicable work budgets and allocation checks. The six classes listed below MUST join the maintained budget inventory and invocation in follow-up tooling work.

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
| CONV-201 | No existing test mapped. **Planned:** `test_pr_mutation.py` disposition-file fixtures: valid entry, missing/unknown/empty/mistyped fields, pattern characters, duplicate identity, wrong classification, short or placeholder justification, malformed revision. | proposed |
| CONV-202 | No existing test mapped. **Planned:** `test_pr_mutation.py` CLI fixtures: killed, dispositioned and failing changed-line mutants, line-shifted identity, unconfirmed/contradicted/stale/not-judged entries, re-review, invalid file, incomplete runs. | proposed |
| CONV-203 | No existing test mapped. **Planned:** `test_pr_mutation.py` added-entry summary, no out-of-band relaxation, removal never accepts a mutant; review acceptance. | proposed |
| CONV-301 | No existing test mapped. **Planned:** Budget inventory reconciliation against executable work-counter assertions; execute the six mapped suites and the Work budgets block for affected product changes. | proposed |
