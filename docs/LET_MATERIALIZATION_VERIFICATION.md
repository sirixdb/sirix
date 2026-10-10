# Let-bound FLWOR materialization

Repeated references to a lazy let-bound FLWOR result previously traversed its source for every
reference. SH1 Q3 references `$rows` three times, for minimum, maximum and distinct-price count.
`LetMaterializationStage` now marks a repeatedly referenced, proven-pure `PipeExpr` initializer
with `SIRIX_MATERIALIZE_LET`. `SirixTranslator` compiles that marker to `MaterializeExpr`, which
evaluates the source once while the binding evaluates. An exact `ItemSequence` result is already
eager and is reused directly, including a covered-row projection buffer; other results go through
Brackit's `ExprUtil.materialize`. Subclasses of `ItemSequence` still take that path because they
may override iteration with lazy work. The resulting sequence belongs to that binding tuple.
The compiled expression retains its source expression and immutable purity metadata, so each
outer tuple and each later query evaluation evaluates a fresh binding.

There is no cross-evaluation memo, deferred replay or invalidation. Buffering retains all result
items until that binding is no longer referenced. Empty and singleton results use Brackit's
existing representations. The materialization utility closes the source iterator on success and
failure; a failed evaluation leaves no saved result or exception in the compiled expression.

## Admission

The final optimizer stage counts references by resolved binding identity, rather than variable
spelling. It uses the existing read-only expression classifier, then proves lazy variable-source
dependencies. Global initializers are read from the enclosing module's prolog: the optimizer is
invoked on individual bodies, which do not contain those declarations. Engine-produced bindings
inside the candidate source, including join outputs and filter context items, are covered by the
same source proof. Dependency cycles or exhausted proof budgets decline admission.

Physical PATH, NAME and CAS `IndexExpr` plans count as stored reads even when childless.
Their database, resource, revision and index-type properties must be concrete; CAS bounds
must be atomic values. Unproven physical inputs decline admission. These plans use the same
provider and result-lifetime guards as document calls, including through global and captured
producer dependencies.

Consumer admission requires a terminal return with scalar reductions and at least one unconditional
full-consuming reduction before the first caller-visible result, including for pure local sources.
Calls containing argument placeholders, such as `sum($rows, ?)`, create partial functions;
they do not establish consumption and are excluded by the purity proof as well.
Later members of a lazy comma sequence do not establish this traversal. Eager constructors and
full-consuming functions can complete a nested sequence before exposing its result; a full reduction
in the first result member also establishes demand. Existence-only consumers stay lazy; `exists`
remains eligible alongside full-consuming reductions. The proof follows known consuming ancestors; positional filters,
quantified inputs and predicates, skipped branches and partial-consuming calls do not establish
full consumption. Direct composite returns,
aliases, deferred bodies and bindings retained across further iteration stay lazy. Global-dependent
bindings and all stored reads, including transitive producers, require an eager scalar result or
constructed result object or array, so every dependent use finishes before a result reaches the
caller. Latest-revision reads can change after a commit even with the stock provider. Q3's aggregate
object satisfies this consumption boundary. Unboxing an eagerly constructed array preserves scalar
result sequences; the work-budget fixtures use this form with their original read bounds, and
`exists` exercises partial source consumption before the remaining reductions complete.
The same purity proof and input guards cover result fields evaluated before those reductions.
Singleton array results remain wrapped as one sequence item, preserving their cardinality.

Updates, unknown calls, non-built-in functions, function parameters, external
variables, unresolved context items and clock/timezone calls are conservatively excluded.
The purity proof resolves functions through the same static-context registry as translation;
a registered Java function that shadows a built-in name is also excluded.
Named and inline function parameters are distinguished from same-named globals before lookup,
including dependencies reached through aliases. Captured outer let and for values are checked
at each binding using the existing scalar-input guard: only empty or atomic values permit
materialization. Opaque sequences, objects and arrays retain evaluation per reference without
inspecting their fields. Global-default proofs exclude constructors and stored reads,
including those reached through aliases: constructed values may have escaped to a consumer,
while stored reads can observe later revisions.
Caller bindings can override even non-external global declarations in Brackit. Immutable metadata
records the global defaults used by each proof; the materializing expression checks for overrides
once when that binding evaluates and leaves an overridden source lazy. External declarations,
including ones with defaults, are never statically admitted. This is a purity check, with no saved
sequence or invalidation state. Resolved user functions that shadow JSON read functions and
implicit context arguments without a source proof are also excluded. A source that reads stored
JSON is materialized only with the stock `BasicJsonDBStore`; a custom provider keeps the generic
per-reference behavior because its document and field reads have no purity proof. The shared
runtime guard also requires stock implementations for every currently registered collection:
`addDatabase` can install a decorator inside a stock store, including after compilation. The
guard also rejects a non-stock default JSON collection supplied by the query context, including
one installed after compilation or selected by `jn:collection()` or an empty name. It checks
provider metadata without opening documents or inspecting fields. Registry mutations maintain
a non-stock collection count, so the stock per-row guard is constant-time and allocation-free.
Registration increments that count before publishing an unproven provider; replacement,
removal, closed-database cleanup and drop decrement it after removing that provider. This
conservative publication order prevents admission while an unproven provider is visible;
the count is registry metadata, not a saved let sequence or cross-evaluation cache.

Single-reference bindings remain lazy. Admission currently targets lazy FLWOR (`PipeExpr`)
initializers; it does not add materialization to scalar lets or module-global declarations.
`sirix.optimizer.materializeLets=false` disables admission for fresh compilations. As with other
optimizer controls, use a fresh compile chain for A/B comparisons to avoid an already-cached plan.

## Behavioral and work evidence

The initial Q3 regression tests were added and executed before implementation. In two evaluations of the
same compiled Q3 over 100 stored rows, the baseline read **600** source-array items, and the
fixed executable plan reads **200**: exactly one 100-row scan per evaluation. The budget test
also executes the disabled-stage baseline and requires its 600 reads, proving the counting seam
observes all three references. The Q3 plan must carry exactly one materialization marker; the
baseline carries none.

`LetMaterializationWorkBudgetTest` additionally checks empty, singleton, 10,000-item and partially
consumed results against the disabled-stage plan, constant first-item work for existence-only
consumers of a 10,000-row source, nested bindings, and correlated rebinding for
three outer tuples. Skipped quantified inputs and predicates retain constant source work. Filter
cases compare enabled and disabled source reads without assuming that the generic filter skips
its reduction: the published runtime also fully consumes the reported positional-filter case
with materialization disabled. Those bindings remain lazy and add no source work.
Early-close cases use `Query.execute()`, consume one result item and close the iterator. A counting
decorator on the translated arithmetic expression observes actual pure-local source evaluations:
an existence prefix performs one evaluation, while eager constructors and first full reductions
retain one complete traversal per binding. Nested lazy sequences and deferred bodies preserve
the disabled plan's prefix work across repeated executions of the same compiled query.
Restoring the previous admission rule made the existence-prefix budget fail at 20,000 source
evaluations instead of 2 across two executions, proving that the counter detects full buffering.
`LetMaterializationTest` checks shadowing, single/unused bindings, unproven
calls, effectful lazy dependencies, caller-dependent function parameters, global overrides, external defaults, shadowed functions,
implicit context arguments and positional `allowing empty` bindings. `MaterializeExprTest`
checks repeated binding evaluations, repeated result consumption, cursor closure, failure recovery,
rejection of updating expressions, eager-buffer identity, singleton-array cardinality and lazy
`ItemSequence` subclass consumption. `EagerLetMaterializationTest` observes the executable
covered-row serving route and requires the binding to reuse its exact buffer, with unchanged
output and a fresh buffer on each query evaluation. Custom-provider reads retain their original
per-reference execution; the stock-provider Q3 budget still requires exactly one scan per binding.
`IndexedLetMaterializationTest` verifies executable PATH and NAME index selection, latest-revision
reads after commits between streamed results, eager indexed positives and dependency/provider guards.
Partial-function regressions invoke two returned reductions after separate commits and require
the generic values 2 then 3. Default-provider and registered-function regressions preserve
per-reference values 3 then 7. The
[budget inventory](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md#what-is-here)
owns the provider-classification work bounds checked by `ProviderPurityWorkBudgetTest`.
The initial targeted run passed **18 tests**. After the initial additional purity cases, that targeted
run passed **27 tests**, including a custom-provider regression that first failed with one lookup
instead of the required two.

## SH1 Q3 measurement

Measured on 2026-10-06 for the initial implementation, using a private copy of the SH1 t100k
`t100k-hot-44fc2f4afe01-nodiffs` database. Both variants use the normative `BitemporalQueries`
Q3 and the same production compile chain/runtime; only the materialization stage is toggled.
Each variant has five warmups and nine measured executions, with alternating order. Compilation
and canonicalization are outside the measured interval; complete query serialization is inside it.
All 28 executions, including warmups, are canonicalized and byte-compared to the independent SH1
oracle (`60121\t60321\t3\n`). No wall-clock threshold is asserted in the automated tests.

| Variant | Warm median | Q3 source scans per binding |
|---|---:|---:|
| Materialization disabled | 228.799 ms | 3 |
| Materialization enabled | 74.553 ms | 1 |

The measured reduction is **3.07x**. These historical A/B figures include the cheap-first predicate
ordering and dependencies used on that measured head; they were not remeasured after the rebase
or subsequent guard and eager-buffer fixes. They do not compare against the older profiling
report's 1,318 ms baseline. Timings on this shared laptop are evidence, not a CI latency guarantee.

The [probe source](../bundles/sirix-query/bench/bitemporal/evidence/let-materialization-2026-10-06/Q3Probe.java.txt)
and [samples](../bundles/sirix-query/bench/bitemporal/evidence/let-materialization-2026-10-06/q3-samples.tsv)
are retained with the benchmark evidence. The oracle TSV SHA-256 is
`2b33db4275bb42f191ca5d8096a3be65536d2dff0d34e5b49cc33a4e553eaefd`.
Source metadata, regression XML and logs are under `build/let-materialize/`. The verification plan there records the initially empty private Maven
repository, `build/let-materialize/m2-private`, used for every Gradle command. Published Brackit
snapshot `1.0-alpha10-20261006.152144-93` was resolved, avoiding the stale local `~/.m2` snapshot.
For that measurement and initial validation, all Gradle and benchmark JVMs ran under the prescribed
memory/lock limiter. Query test forks used
`-PtestHeapMin=256m -PtestHeapMax=2g`.

## Initial suite validation (2026-10-06)

For the initial implementation, the full sirix-core suite reports **13,156 tests**, 78 skipped,
with zero failures or errors.
The fresh full sirix-query suite for that implementation reports **3,018 tests**,
12 skipped, with zero failures or errors. Both use 2 GiB test forks. The final targeted regression
run and the final query run also pass `:sirix-query:spotlessCheck`.

Formatting was run separately before the final query invocation. An earlier long combined run
kept its initial Gradle source snapshots after late purity fixes, so its earlier-class query result
was superseded by the fresh final query run. A missing `close()` in a new test iterator was caught
by that fresh compilation and corrected before the 27-test targeted pass and final full suite.

The explicit *Work budgets* block from `docs/VERIFICATION.md` passed with unchanged bounds:

- `sirix-query`: **43 tests**, 5 skipped, zero failures or errors.
- `sirix-core`: **47 tests**, 0 skipped, zero failures or errors.

Logs: `build/let-materialize/full-tests.log` (full core), `full-query-final.log` (final full
query), `final-targeted-green.log`, `q3-benchmark-final.log` and `final-budgets.log`. The final
budget invocation also passes `:sirix-query:spotlessCheck`. No no-mistakes pipeline was started
during implementation: the committed branch is handed off to firstmate before that stage.

## Post-main-merge validation (2026-10-09)

Main was merged with all budget inventory rows retained. The merged runtime includes runtime-revision
CAS routing and XML publication changes. Fresh compilation, focused materialization tests, Q3 and
query budgets, runtime-revision CAS tests, mixed valid-time mutations and bitemporal integration
checks passed: **450 tests**, five skipped, zero failures or errors. Core work budgets also passed:
**229 tests**, zero skips, failures or errors. Query `spotlessCheck` passed. No budget bounds changed.

The SH1 Q3 A/B probe was rerun on that rebuilt runtime, using the same private database and oracle,
five warmups and nine measured runs per variant, and alternating order. All **28 runs** were
oracle-exact. Warm medians were **225.246 ms** with materialization disabled and **74.319 ms**
enabled (**3.03x**). The [samples](../bundles/sirix-query/bench/bitemporal/evidence/let-materialization-2026-10-09/q3-samples.tsv)
and [metadata](../bundles/sirix-query/bench/bitemporal/evidence/let-materialization-2026-10-09/metadata.txt)
record this separate measurement; the historical October 6 results above remain unchanged.

All build, test and benchmark JVMs used the memory/lock limiter. The initially empty private Maven
repository was `build/let-materialize/m2-postmerge-private`; the resolved published Brackit snapshot
was `1.0-alpha10-20261008.110148-96`, SHA-1
`218ea254a0e7d9c65841fdf98a1a19501c37dab4`. Test forks used a 2 GiB maximum heap.
Local logs and regression XML are retained under `build/let-materialize/postmerge-*`.
