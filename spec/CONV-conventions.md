# CONV — Conventions and conformance

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 document conventions, traceability rollout and the accepted work-budget classification. This does not implement the proposed trace tool or change CI. Code fields for process requirements point to the authoritative workflow artifacts; no product method enforces them.

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

A feature or behavioral fix MUST state behavior, invariants and errors and derive tests from those requirements before product implementation. Each test oracle MUST distinguish at least one plausible violation independently of implementation details. Changed logic MUST receive mutation testing; each surviving or uncovered mutant needs either a stronger test or an explicit equivalent/unreachable justification. Bug fixes MUST include the postmortem template, regression and lesson required by CONTRIBUTING.

### CONV-104 — Traceability rollout

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [CONTRIBUTING.md](../CONTRIBUTING.md); [VERIFICATION.md](../docs/VERIFICATION.md)
- **Verified by:** No existing test mapped. **Planned:** SpecTraceContractTest plus fixtures for each failure, annotation discovery, default-lane versus opt-in evidence and PIT selection by requirement.
- **Evidence class:** doc-only (unverified automation); decided target has unverified implementation gaps
- **Gap/note:** spec_trace.py, @Verifies, CI wiring and test annotations are intentionally planned: this task changes Markdown only.

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
| CONV-301 | No existing test mapped. **Planned:** Budget inventory reconciliation against executable work-counter assertions; execute the six mapped suites and the Work budgets block for affected product changes. | proposed |
