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

The Sirix stage runs before Brackit's pipelining. It marks the membership selection, and
`SirixPipelineStrategy` compiles it into a physical `HashMembershipJoin` operator. The operator
returns the original outer tuples: order, multiplicity, and node identity are preserved.

Each operator cursor owns its key sets and inner iterator. A newly opened cursor starts without
keys, even when it iterates a lazy result retained by a declared variable. No mutable execution
state lives in the compiled expression, query context, or pipeline tuple. Closing or exhausting
the cursor releases its table and inner iterator; failures also close both input cursors.
An early close of a partially consumed result closes an unfinished inner scan.

The build is lazy: an empty outer input never reads the inner relation. Both semi and anti joins
stop at a matching key. Subsequent probes use keys already read or resume that same inner scan;
a missing key completes the scan. Supported homogeneous domains therefore require at most one
inner pass per stable binding in the pipeline, including nested outer loops. There is no
per-row rebuild, and a first-row match against a million-row input still reads one row.
The original `empty`/`exists` predicate also stops at a match. A later individual probe can read
more rows than that predicate would read from the start; the guarantee is one inner pass amortized
over the cursor's probes of a stable binding, not a lower read count for every single probe.

A change of the independent inner binding closes the previous scan and discards its keys. Local
bindings are read from their actual tuple slots, because `BoundVariable` wraps a sequence in a new
`TypedSequence` on each reference. Declared bindings are read from the query context. These reads
identify the active relation within a cursor; they do not retain a lookup across cursors or query
executions. `$inner[]` uses its base binding, since the unboxing expression itself creates a new
sequence on each evaluation.

The key sets occupy no tuple slot, so a downstream spilling `group by` or `order by` can serialize
its ordinary tuples. Pipelines containing membership joins use the sequential cursor strategy,
including when the caller requests block execution or enables morsels. This gives the table one
owner instead of building it independently in each worker. Pipelines without membership joins
retain their existing parallel and vectorized routing.

Integer keys use a primitive long set; string, untyped-atomic and URI keys use codepoint string
keys. Null keys have their own flag and do not force the typed keys off the hash route. Unsupported
numeric promotions, mixed comparison domains, and extraction errors use the original nested
predicate. A retained positive key proves a match; an incomplete set never proves absence.
No coercion to a common floating-point key is attempted. General comparisons remain unchanged.

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

The tests exercise real spill paths and exact output, rather than relying on the syntax of a
lookup expression. They also cover mutable database bindings, failed-execution retries, nested
outer loops, interleaved evaluations, unsupported numeric early exits, and closing a partially
consumed result.

For 128 inner keys and 256 outer items, the anti join reads 128 keys in one scan; the original
predicate reads 24,640: `128 * 129 / 2 + 128 * 128`. A mixed-hit/miss semi join over eight inner
keys reads eight keys overall, compared with 33 for the original predicate. These are work-count
assertions, with no wall-clock bounds.

The local Brackit snapshot has a pre-existing `Analyzer.checkCycle` NPE when compiling the retained
declared-FLWOR regression. That query test is explicitly disabled with the reason. The same
lifecycle is executed directly through Brackit's `DeclVariable` and `PipeExpr`: repeated iteration
opens fresh physical join cursors, mutations of the same source object change the answer, and
interleaved iterations cannot share keys. A query-level UDF returning the FLWOR supplies the
nearest compilable end-to-end regression. No Brackit classes are patched.

## Physical operator validation

The cursor operator supersedes the expression-memo implementations measured below. Measurements
of implementation commit `cddb195dcea3358dd4eb294085f0061241ba72d6`, based on published commit
`333ebe1a1fd5266d391e8ef94b577b293ba13e90`, use freshly loaded stores, the kit's natural publication
batching, and a fresh JVM for each standalone leg. The diagnostic property disables only the
membership rule on the same compiled classes. Each answer is byte-checked against its independent
oracle before its duration is reported.

| Tier | Rule disabled Q12 (s) | Physical operator Q12 (s) | Fresh-process repeat (s) |
|---|---:|---:|---:|
| t25k | 72.748929 | 3.534460 | — |
| t50k | 308.052080 | 4.612219 | — |
| t100k | 1088.563352 | 7.537337 | 6.494075 |

Successive doublings grow by 4.23x and 3.53x with the rule disabled, versus 1.30x and 1.63x with
the operator. The t100k reduction is 144x (168x on repeat). Concurrent work shares this machine;
these runs support the order-of-growth conclusion, while the deterministic work counters prove
the saved inner reads. These measurements precede any subsequent pipeline rebase and do not
describe a different head. Historical expression-memo timings below describe their named commits.

The full query suite passed 1,808 tests with zero failures/errors and seven skips. Its 16 query
work-budget tests passed; the unchanged core inputs retained their green 25-test work-budget
result. A focused 256 MiB run executed all 48 enabled membership tests and all 13 plan serializer
tests, including actual order/group spills. The disabled declared-FLWOR analyzer case is described
above. All 45 ClickBench variant plans are identical with the rule on and off; the full query suite
also passed its ClickBench smoke/acceptance tests. This is plan and correctness evidence, not an
isolated ClickBench timing campaign.

SH1 is 12/12 oracle-exact at t25k, t50k and t100k on this implementation. The source pins,
dependency hash, answer/input hashes, standalone pairs/repeat, all-query observations, and
before/after plans are recorded in
[`physical-operator-validation.json`](../bundles/sirix-query/bench/bitemporal/evidence/q12-membership-2026-10-01/physical-operator-validation.json).
The worker relaunch interrupted t25k's all-query JVM after Q9; Q10–Q12 resumed in a fresh JVM on
the same store and classes. Its standalone Q12 pair was uninterrupted. No physical-operator
t250k result is claimed; that tier's results below belong to the historical expression design.

## SH1 evidence (2026-10-01/02, measured on commit `79042b96a`)

Every number in this section — timings, suite counts, and the campaign source hashes in
`measurements.json` — was measured on commit `79042b96a` and describes that commit only. Later
commits are not covered by these numbers; see [Historical expression implementations](#historical-expression-implementations)
for the superseded designs and [Physical operator validation](#physical-operator-validation) for
the separately pinned cursor implementation.

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

## Historical expression implementations

Later expression-based implementations removed an opaque lookup from tuple slots to fix spilling,
corrected raw-binding identity for local `let` sequences, and added early-exit/fallback behavior.
Those implementations were superseded because a compiled-expression memo could retain keys across
executions of mutable bindings. Attempts to attach the memo to execution contexts also failed for
lazy values retained by declared variables. The physical operator above removes that machinery.

The historical `remeasured_t100k_at_73d6e5daf` block records 1039.163508 s with the rule disabled,
5.267062 s enabled, and 5.532705 s on a fresh-process repeat. Its answer hash is
`b86458c4cc53e0102a04652690344f1d319e4bb16a667e2770a6dbb722429068`. These are measurements of
`73d6e5daf`, not the physical operator. Earlier source hashes, plans and validation records remain
in the evidence directory with their original commit labels.
