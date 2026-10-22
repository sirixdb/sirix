# Routed membership: CodeFactor repair evidence

Measured on 2026-10-22 for PR #1308. The hosted CodeFactor diagnostic is login-gated;
complexity and duplication are inferred local causes, not a confirmed hosted rule.
Publishing and checking CodeFactor on the repaired commit belong to the outer CI pipeline.

The [method-level CSV](routed-membership-codefactor.csv) retains all methods and constructors
touched by the PR, including removed methods, formatting-only changes, and the repair's
new helpers. Its snapshots are:

- `base`: `4216946bf8e80e68a965bd57cf23f3763505c0f7`.
- `before`: `365e79376f89046227ca871c75023e24c3b6c3ce`.
- `after`: the working tree containing this helper-extraction repair.

## Measurement

PMD 7.13.0 analyzed all 11 changed Java files with `CyclomaticComplexity`
(`methodReportLevel=1`, `classReportLevel=100000`) and `CognitiveComplexity`
(`reportLevel=1`), with caching disabled. Cognitive zero means no diagnostic at
threshold 1. No processing errors occurred. These are PMD's default metrics;
cyclomatic scoring does not count boolean operators in every expression context.

PMD's rules omit compact record constructors. The two changed compact constructor
bodies (`GroupRouting` and `GroupMembership`) were also analyzed as ordinary
constructors with explicit record parameters and identical bodies. Their CSV rows
use the original class and canonical signature. Duplication always uses real sources.

PMD CPD 7.13.0 analyzed all Java sources under `bundles/sirix-core/src` and
`bundles/sirix-query/src` independently for each snapshot, at a minimum of 100 tokens.
Each method's duplication column counts the union of source lines overlapping CPD
fragments, clipped to that method's declaration/body. `absent` denotes an absent
signature, rather than zero work or complexity. The removed `filterByMembership`
and its replacement `membershipValues` therefore have separate rows.

| Method | Base cyclomatic/cognitive | Before | After |
| --- | --- | --- | --- |
| `groupByAggregate` | 692 / 2173 | 705 / 2190 | 680 / 2150 |
| `serveRouted` | 21 / 11 | 30 / 18 | 12 / 11 |
| `filterMembership` | absent | 10 / 20 | 7 / 10 |
| `routedMembershipMasksMatchSemiAndAntiJoinsWithoutAnotherKeyArray` | absent | 15 / 39 | 7 / 14 |
| `denseSelectiveMembershipReusesBoundedSnapshots` | absent | 11 / 30 | 2 / 1 |
| `refusedMembershipReusesEvaluationColumnsAndSkipsEmptyAntiReads` | absent | 12 / 30 | 3 / 3 |

Every helper introduced by this repair has cognitive complexity at most 14.
Two new CPD fragments (105 and 102 tokens) in the membership work-budget tests
were removed by sharing source-key lookup and sorted cost aggregation. Their
callers and helpers now have zero overlapping duplicate lines at this threshold.
The large aggregation method's 111 overlapping duplicate lines are unchanged
from the PR base.

## Behavior and work preservation

Required-field collection retains field order, deduplication calls, computed-lane
resolution and the original single `ArrayList` allocation. The scalar, computed,
conditional-key, and membership consumers receive the same field list.

Routed predicate preparation retains the same eligibility checks, cache lookup,
column reads and callback order. Each ordinary decline increments its original
counter once and preserves its diagnostic. All-kept selections retain the resident
fill, and dense selective selections retain bounded numeric snapshot reuse. Sparse
and late-refused admission publish prepared columns before fallback; empty anti
membership remains normalized away. No cache bounds or admission policies changed.

The mask-word helper receives the existing arrays and result buffer. It makes the
same presence checks and membership probes, with no per-row helper call, extra
allocation, key remapping, read or decode. Semi/anti and empty-inner behavior share
the same implementation for dense and exception-key layouts.

Test extractions retain the existing fixture lifetimes, case order, assertions and
work bounds. The shared source-key lookup still executes before the caller's
assertion/sort sequence; the shared sorted-sum call supplies identical query arguments.
Static metrics are maintainability evidence, not behavioral tests or latency evidence.

## Local verification

All Gradle and analyzer invocations ran directly through the requested `heavy.sh`
wrapper. Project Gradle commands used the requested private Maven repository with
`-PuseMavenLocal=true`. No timed benchmark campaign was run.

The complete Work budgets commands from `docs/VERIFICATION.md` ran, together with
`RecordKeySetPredicateTest`, `MembershipProjectionFallbackTest`,
`NumericMaskedColumnCacheTest` and `ValidTimeContainerMoveQueryTest`. Core tests had
318 passes, plus seven auxiliary passes. The final query rerun had
307 passes and five skips, plus 16 auxiliary passes, including all 54
`IndexRoutedGroupWorkBudgetTest` cases. Spotless checks for both modules, Checkstyle
on all 11 changed Java files, and `git diff --check` passed. The earlier combined
invocation completed all tests and failed only on wrapping the final test helper;
the corrected wrapping passed in the final rerun. No budget or assertion changed.

Raw PMD/CPD reports, source snapshots, rules, comparison scripts and Gradle logs
remain in `.gradle/ci-codefactor/metrics/` in this worktree. The CSV retains the
method-level measurements in versioned project evidence.
