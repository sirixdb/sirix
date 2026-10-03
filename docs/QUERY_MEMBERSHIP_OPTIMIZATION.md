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

The Sirix stage runs before Brackit's pipelining. It replaces the predicate with a membership
probe that owns an opaque lookup object as a nested child expression. No outer rows means no source
read.

The inner side is read **lazily** — nothing until an outer row probes — and its iterator is opened and
closed inside the probe that needs it. It is never parked between probes, and that is a correctness
requirement, not tidiness: Brackit's `fn:empty`/`fn:exists` (both `EmptySequence.execute`) call
`Iter.close()` on the normal path *and* from a catch-all handler, so the plan this route replaces
releases the inner cursor the moment it answers. A parked iterator would release a Sirix stream or
transaction later than that, and `Expr` has no teardown hook to release it at — an earlier revision of
this note claimed the pause matched Brackit's behaviour, which was simply wrong.

That fixes the shape of each direction. An **anti-join** reads the relation through to the end in the
first probe that needs it, and the completed key set then answers every later probe without touching
the source. A **semi-join** stops at its own key, because one match answers the predicate — the early
exit the unoptimized plan has, where `TableJoin` hashes the one outer key and streams the inner side
and `fn:exists` stops at the first match, so `exists(...)` over a million inner rows visits one row
where draining first would have visited all of them. Having stopped, it gives the partial keys up
rather than parking the scan: a later probe whose key the retained set already contains is answered
from it — a key in the set was indexed from a real inner row, so it proves a match however little of
the relation was read — and only a key the set does not contain goes back to the original predicate.

**The bound is amortised, not per probe.** At most one pass over the inner relation for the whole
lifetime of a lookup, however many outer rows probe it. It is not a per-probe bound, and an earlier
revision of this note claiming "neither direction reads more of the inner relation than the plan it
replaces" was wrong: the plan this replaces also stops early. Brackit's `EmptySequence` backs both
`fn:empty` and `fn:exists` and pulls one item before closing, so each of *its* probes reads only up
to its own match — which is exactly why the rule-off counter below is `128 * 129 / 2 + 128 * 128` and
not `256 * 128`. A single probe here can therefore read more than that plan's probe would: one outer
row whose key matches the first inner row costs the original one row and costs this route the whole
relation. The win is sharing that one pass across many outer rows, which is the shape the rule exists
for — Q12 probes a 100k-row history from a large outer side — and stopping the anti-join at its match
would hand that shape straight back to a per-row scan. Reading further than the original also means
an error sitting past the original's early exit can surface here when it would not have there. A probe
whose key the completed set does not contain, and whose domain cannot be compared with it, still
delegates to the original predicate.

The lookup occupies **no pipeline tuple slot**. That is a hard requirement, not a preference: a
spilling `group by` or `order by` serializes every slot of every tuple it carries, Brackit's
`TupleSerializer` writes only its own atomic types, and the lookup is not one of them. An earlier
revision hoisted the lookup into a generated `let` binding before the outer `for`; a `group by`
that spilled then failed the whole query with `bit:BIDY0005: Serialization of item type 'item()'
not implemented yet.` Nesting the lookup inside the probe removes the slot, so nothing about the
tuples reaching `group by`, `order by` or the block `GroupBy` differs from the unoptimized plan.

One build per enclosing binding is kept without a slot by memoizing on the *scope variable* — the
independent variable the inner source reads, which the admission rule already requires. The memo is
keyed on that variable's **binding**, not on the value a reference to it returns, and that
distinction is the whole correctness of the scheme: `BoundVariable.evaluate` runs
`TypedSequence.toTypedSequence`, which allocates a fresh wrapper for every value that is not a
single `Item`, so a memo keyed on a reference's result never hits for a multi-item inner relation
and rebuilds the hash set for every outer row. Q12's `let $new := local:slice(...)` is exactly that
shape. The binding is therefore read at its source: a local `let`/`for` binding straight out of its
tuple slot (the translator registers the lookup as a `Reference` on that binding, so it receives the
same slot position every other reference gets), and a module-level variable from the query context.
Both are the same object for every row of the outer `for` and a different object once an enclosing
binding moves on. Note the asymmetry a slot read fixes: a `for` slot holds a single `Item` and was
always stable, while a `let` slot holding a sequence was not.

`$inner[]` still may not be the key — it allocates a fresh unboxing sequence per evaluation — which
is why the key is the base variable the admission rule pins down rather than the source expression.
Reuse happens only when the bound object is the same object, which cannot differ in content, so a
hit is always sound and a miss only costs a rebuild. Enclosing bindings and subsequent executions
get fresh lookups; the memo holds one entry, so concurrent block-pipeline workers sitting on
different enclosing bindings rebuild instead of sharing, and each worker is always handed the lookup
built from its own binding. The memo holds that entry **softly**, and the lookup drops the tuple it
was built from as soon as its keys exist, so a finished evaluation's key set and the outer row's
database items are collectable rather than pinned for the lifetime of the caller's `Query`.
A probe against a completed set reads it without taking the monitor — the volatile `complete` flag
publishes the sets — while a probe that still has to read the inner side holds it, so one scan is
shared rather than raced.

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
cardinality/type errors, grouping, enclosing scopes, and repeated execution of one compiled query.
It also counts consumed inner items, iterator closes and key reads, pins one build for a locally
let-bound and a Q12-style function-call-bound inner relation, and forces both spilling operators:

- `group by`, with `-Dio.brackit.query.groupby.memory_budget=1`, its configurable spill trigger.
- `order by`, which has no such property and sizes its budget as `Runtime.maxMemory()/4`, so
  `aSpillingOrderByAfterAnAntiJoinCompletes` runs only under a focused small-heap invocation:
  `./gradlew -PtestHeapMin=128m -PtestHeapMax=256m :sirix-query:test --tests
  io.sirix.query.compiler.optimizer.HashMembershipStageTest`. It proves the spill is real rather than
  assumed by pointing `java.io.tmpdir` at a non-writable directory, where only a spill can fail
  (`TupleSort` creates its run files there), and then asserts the exact descending answer and route
  admission against a writable one. Against the superseded let-bound plan it fails with
  `bit:BIDY0005`, which is reachable only from `TupleSerializer` inside `TupleSort.writeRun` — so
  that failure is itself proof that the sort spilled. It is skipped, not silently passed, at the
  suite's normal heap.

The block `GroupBy` shares the same `TupleSerializer`; the slot-shape assertion in
`theLookupNeverOccupiesAPipelineTupleSlot` covers it by pinning the property that makes all three
safe.

For 128 inner keys and 256 outer items of the **anti-join** shape, the enabled rule visits exactly 128
inner items and closes one build iterator. Disabling it produces the same answer but visits 24,640
inner items: `128 * 129 / 2 + 128 * 128`. The work bound is stated on the anti-join because that is
the direction that owns a shared pass; a semi-join that matches on its first probe hands the
remaining outer rows back to the original predicate by design, so it carries the
never-worse-than-original bound instead, asserted by running the same query with the rule on and off
and comparing both counters. The counter assertion was run with the rule disabled and failed on
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

The campaign section above is a closed record of `79042b96a`. Seven changes landed after it — the
null key, the `sdb:explain` range, the lookup leaving the tuple slot, the memo key, the retention
release, the probe-scoped iterator with its terminal exit, and consulting the retained keys before
delegating — so its
suite counts (`1,774 tests`, `16 membership tests`) and the `MembershipIndexExpr.java`,
`HashMembershipStage.java` and `SirixTranslator.java` entries in `candidate_sources_sha256` describe
that commit, not HEAD. They were deliberately left as recorded rather than re-written without a new
measured run.

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

The lookup moved out of the pipeline tuple into the probe, because a spilling `group by` serializes
every slot it carries and killed the query outright — see *Plan and invariants* above for the
mechanism. Covered by `aSpillingGroupByAfterAnAntiJoinCompletes`,
`aSpillingGroupByKeepsExactCountsForManyGroups`, `anOrderByAfterAnAntiJoinCompletesAndKeepsTheRoute`
and `theLookupNeverOccupiesAPipelineTupleSlot`. All three of the first, second and fourth fail on the
pre-fix plan — the two spilling ones with the `bit:BIDY0005` serialization error, the fourth because
one variable was still bound to a lookup.

The memo key was then corrected. Nesting the lookup in the probe meant re-deriving "one build per
enclosing binding" by hand, and the first attempt keyed the memo on what a *reference* to the scope
variable returned. `BoundVariable.evaluate` runs `TypedSequence.toTypedSequence`, which allocates a
fresh wrapper for every value that is not a single `Item` — so for a locally let-bound multi-item
inner relation the memo never hit and the hash set was rebuilt for **every outer row**. That is
Q12's own shape (`let $new := local:slice(...)`), so the rule's entire purpose was defeated for the
query it exists for, while every work-counting test stayed green because all of them bound the inner
relation as `declare variable ... external`, which takes the context path that already worked.
Keying on the raw binding — the tuple slot for a local, the context value for a module-level
variable — fixes it. `aLocallyLetBoundInnerRelationIsBuiltOnce` and `aQ12StyleLocalSourceIsBuiltOnce`
pin both shapes and both fail before the fix at 32,768 inner reads for 256 outer rows. The second of
those counts key reads on the records rather than sequence iterations, because Brackit materializes a
`let` over a function call and a counting sequence can no longer see the rebuilds through it.

The lookup also no longer pins a finished evaluation's state: it drops the tuple it was built from as
soon as its keys exist, and the memo holds its single entry softly, so the key set and the outer
row's database items are collectable instead of being retained for the lifetime of the caller's
`Query`.

### Re-measured done bar (2026-10-03, t100k)

The plan tree changed, so the campaign timings above do not describe this state and are preserved only
as history — `t100k-after.plan.txt` / `t250k-after.plan.txt` still show the superseded
`LetBind sirix:membership0`. The evaluation algorithm then changed twice more (an incremental key set,
then a probe-scoped iterator), so the 2026-10-02 re-measurement is superseded too and is kept under
`remeasured_t100k_after_memo_fix` with that note. Q12 was measured again on the current state, against
a store freshly loaded with `BitemporalSirixLoadMain t100k` from the unmodified kit event stream with
one commit per publication and no batching override (load: 67.071 s):

| Q12 at t100k | seconds | rows | oracle |
|---|---:|---:|---|
| baseline `8aa9f0d9e` (historical) | 1069.754587 | 4 | exact |
| superseded let-bound plan `79042b96a` (historical) | 8.443716 | 4 | exact |
| **this state, rule disabled** | **987.999106** | 4 | exact |
| **this state, fresh process** | **5.825218** | 4 | exact |
| **this state, repeat** | **6.376734** | 4 | exact |

The rule-disabled leg is the same classes with `-Dsirix.optimizer.hashMembership=false`, the knob this
note documents for disabling just this rule, so it is a true pair on one build rather than a comparison
across two. It executes none of the changed code. All five runs are byte-identical to the independent
oracle `q12.tsv`, and their answer hash
`b86458c4cc53e0102a04652690344f1d319e4bb16a667e2770a6dbb722429068` is the same answer the baseline and
every intermediate plan produced — so the rewrite is answer-preserving across all of them. That is a
170x and 155x reduction against the same build with the rule off, and Q12 is far below XTDB 2.1's
2,360 s at this tier, so the intent's done bar for Q12 is met. The optimized plan is
`t100k-after-memofix.plan.txt`: the lookup sits inside the `Selection`'s probe with `GroupBy` and
`OrderBy` downstream and no membership variable anywhere. Source hashes for the measured state are in
`measurements.json` under `remeasured_t100k_final_head`, computed from the worktree files the run was
built from. These are shared-machine single-process measurements — the machine gate reported
concurrent heavy JVMs from another worktree, and heavy JVMs were serialized through a `flock` so no
two ran at once — so they support the order-of-growth and done-bar conclusion rather than small
percentage comparisons. Only Q12 at t100k was run.

The authoritative suite result for HEAD is this branch's test step, not the counts above.
