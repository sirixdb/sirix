# Equality membership planning

`HashMembershipStage` handles single-equality semi/anti-joins in a `where` clause:

```xquery
let $new := local:slice('contracts', $D, $V)
for $a in local:slice('contracts', $A, $V)
where empty(for $b in $new where $b.id eq $a.id return $b.id)
return $a
```

The equivalent `exists(...)`, `some ... satisfies ...`, and their `not(...)` forms use the
same path. This is an optimizer rule, not a special case for a collection, field name, or benchmark.

## Plan and invariants

Before the rewrite, Brackit recognizes the inner equality as a hash join, but that join lives
inside the predicate's nested pipeline. Each outer row creates a new cursor. On the SH1 plan,
cost reordering hashes the one outer key and scans the inner relation until a match. The total
inner input visits therefore grow quadratically even though the plan contains a hash join.

The Sirix stage runs before Brackit's pipelining. It inserts a private let binding immediately
before the outer `for`, and replaces the predicate with a membership probe. The binding creates
an opaque lookup object; the first probe builds its hash set. No outer rows means no source read.
The generated variable cannot collide with user variables. The lookup belongs to the enclosing
tuple, not the compiled query, plan cache, thread, or database session. Enclosing bindings and
subsequent executions get fresh lookups. Initialization publishes immutable sets safely to
parallel readers; ordinary probes do not synchronize. Build iterators close immediately.

The admission rule requires:

- One ordinary outer `for` immediately followed by the membership `where`.
- An independent inner variable, optionally unboxed with `[]`.
- One ordinary, untyped inner binding and one value equality (`eq`).
- Variable or direct record-field keys on the two loop variables.
- For `empty`/`exists`, a return of the inner item or its matched key. Returning another field or
  an empty expression cannot be reduced to existence of an equality match.

Hashing uses a primitive long set for 64-bit integral keys and a string set for codepoint string
value equality, including its untypedAtomic/anyURI promotions. A JSON `null` key joins neither set
and sets one flag instead, because value equality on null is total: `null eq null` holds, `null eq`
any other atomic is false, and neither raises an error. A null therefore stays on the hash route
rather than reverting the whole query to the per-row nested plan. Missing keys do not match;
inner duplicates do not multiply rows; outer order and duplicates are preserved. Other types,
mixed domains, unsupported probe types, and speculative extraction errors use the original
compiled predicate. In particular, floating/decimal promotion is not approximated with a lossy
common hash key. The fallback remains visible in the optimized AST.

General comparisons, typed/positional/allowing-empty inner loops, correlated inner sources,
arbitrary return expressions, and additional predicates keep their original plans. The separate
multi-predicate nested-FLWOR correctness work and join-key preference rule are not prerequisites.
Set `-Dsirix.optimizer.hashMembership=false` to retain the original plan for diagnostics.

Brackit's existing NaN comparison behavior is outside this rule: its value comparator considers
NaN equal to NaN, unlike [XQuery numeric equality](https://www.w3.org/TR/xpath-functions-31/#func-numeric-equal).
This was reproduced on unchanged main. The optimizer delegates these unsupported keys to the
original predicate; it does not modify or shadow Brackit classes. A differential test prevents
this fallback from silently acquiring another equality implementation.

## Deterministic regression coverage

`HashMembershipStageTest` checks hand-computed semi/anti-join answers, missing and empty keys,
inner/outer duplicates, numeric precision and promotion, string promotion, nulls, date keys,
cardinality/type errors, grouping, generated-name collisions, enclosing scopes, and repeated
execution of one compiled query. It also counts consumed inner items and iterator closes.

For 128 inner keys and 256 outer items, the enabled rule visits exactly 128 inner items and closes
one build iterator. Disabling it produces the same answer but visits 24,640 inner items:
`128 * 129 / 2 + 128 * 128`. The counter assertion was run with the rule disabled and failed on
that count. The same bound is asserted for an inner side whose keys include one `"id": null`
record, which pins the null key to the hash route rather than the fallback. There is no wall-clock
assertion.

## SH1 evidence (2026-10-01/02, measured on commit `79042b96a`)

Every number in this section — timings, suite counts, and the source hashes in
`measurements.json` — was measured on commit `79042b96a` and describes that commit only. Later
commits on this branch are deliberately not re-measured here; the addendum below records what
changed after it and what covers it instead.

Baseline: Sirix main `8aa9f0d9e`, Oracle GraalVM Java 25.0.3, local Brackit
`1.0-alpha10-SNAPSHOT`. The kit's original query texts, inputs, independent TSV oracles,
VALIDTIME indexes, strict half-open residual, grouping, and one commit per publication were
used unchanged. Diff-sidecar storage is off, as specified by the kit. T50k uses the documented
scale extension (50,000 contracts / 5,000 products / 1,000 suppliers) in a harness-only copy of
`BitemporalSchema`; production schema and kit files are unchanged.

Each standalone Q12 timing is from a fresh JVM with `-Xms512m -Xmx6g`, includes compilation,
execution, serialization and canonicalization, and is reported only after byte equality with
the independent oracle. These are shared-machine diagnostic measurements, not an isolated,
paired ten-round engine comparison.

| Tier | Q12 before (s) | Q12 after (s) | Fresh-process repeat after (s) |
|---|---:|---:|---:|
| t25k | 62.858876 | 4.151746 | 2.836406 |
| t50k | 255.107601 | 4.770234 | 3.921180 |
| t100k | 1069.754587 | 8.443716 | 7.298670 |
| t250k | 5947.946397 | 10.363926 | — |

Baseline growth is 4.06x and 4.19x for successive doublings. With the rewrite it is 1.15x and
1.77x, including fixed cold compilation/initialization work. At t100k the measured reduction is 127x
(147x for the repeat). At t250k the reduction is 574x. From t100k to t250k, baseline time
grows 5.56x for 2.5x the data; optimized time grows 1.23x. The work counter supplies the
hardware-independent linear-work evidence.

The [evidence directory](../bundles/sirix-query/bench/bitemporal/evidence/q12-membership-2026-10-01/)
contains the exact standalone runner, before/after optimized t100k and t250k plan trees,
input/answer/source hashes, measurements, and validation counts.

SH1 is **12/12 oracle-exact at t25k, t50k, and t100k**. The full `sirix-query:test` task passed
(1,774 tests, zero failures/errors, five skipped), including all 16 membership tests, query work
budgets, and the ClickBench smoke/acceptance tests. `spotlessCheck` passed. The core work-budget
selection passed all 25 tests. All 45 variants of the 43 ClickBench queries have identical optimized
plan trees before/after. This checks route stability; it is not a separate isolated ClickBench timing campaign.

The optional t250k Q12 pair also passed its oracle. All twelve queries were checked at the three
required tiers; only Q12 was run at t250k.
One concurrent validation attempt suffered a kernel-confirmed global OOM on this shared machine:
the full query test JVM and optional t250k loader were killed in the same event. The suite had
recorded 1,026 tests with no assertion failures, including all 16 membership tests. Heavy work
was subsequently admitted through the shared two-slot limiter and the full suite passed on retry.
The t250k retry uses a fresh store; the partial original store contributes no measurements.

## Addendum: changes after commit `79042b96a`

The campaign section above is a closed record of `79042b96a`. Two changes landed after it, so its
suite counts (`1,774 tests`, `16 membership tests`) and the `MembershipIndexExpr.java` entry in
`candidate_sources_sha256` describe that commit, not HEAD. They were deliberately left as recorded
rather than re-written without a new measured run.

A JSON `null` key no longer returns `Keys.FALLBACK`. It sets one `hasNull` flag on the `Keys`
record and a null probe key is answered from that flag, so a single null no longer reverts the
whole query to the per-row nested plan. Covered by `HashMembershipStageTest`:
`nullKeysMatchNullKeysAndNothingElse`, `explicitAndAbsentNullFieldsFollowTheValueComparison`,
`aNullInnerKeyKeepsTheHashRouteAndItsWorkBound`, `aNullProbeKeyKeepsTheHashRouteAndItsWorkBound`,
and `aNullOnlyBuildSideKeepsTheHashRouteAndItsWorkBound`. The last three assert the inner-visit
work bound, so they fail if the route reverts to the fallback. The same change moved the
"no typed keys" decision after the probe key is atomized, so an error the unoptimized plan raises
is delegated rather than answered as a non-match; `outerKeyErrorsAreNotSwallowedWhenNoTypedKeysWereBuilt`
pins that, verified against the plan the rule disables.

`QueryPlanSerializer.resolveTypeName` now bounds the XQExt range against `XQExt.NAMES.length`
instead of a hardcoded last type, so `sdb:explain` names the two membership operators instead of
emitting `Unknown(276)`/`Unknown(277)`. Covered by `QueryPlanSerializerTest.resolveXQExtTypes` and
`serializeMembershipNodes`.

The recorded Q12 timings are unaffected by both: the SH1 keys are contract ids with no null among
them, and plan-tree naming is diagnostic output. The authoritative suite result for HEAD is this
branch's test step, not the counts above.
