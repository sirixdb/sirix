# Let-bound FLWOR materialization

Repeated references to a lazy let-bound FLWOR result previously traversed its source for every
reference. SH1 Q3 references `$rows` three times, for minimum, maximum and distinct-price count.
`LetMaterializationStage` now marks a repeatedly referenced, proven-pure `PipeExpr` initializer
with `SIRIX_MATERIALIZE_LET`. `SirixTranslator` compiles that marker to `MaterializeExpr`, which
fully consumes the result while the binding evaluates, using Brackit's `ExprUtil.materialize`.
The resulting sequence belongs to that binding tuple. The compiled expression retains its
source expression and immutable purity metadata, so each outer tuple and each later query
evaluation evaluates a fresh binding.

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

Updates, unknown calls, user functions without a purity proof, function parameters, external
variables, unresolved context items and clock/timezone calls are conservatively excluded.
Named and inline function parameters are distinguished from same-named globals before lookup,
including dependencies reached through aliases. Captured outer let and for values are checked
at each binding using the existing scalar-input guard: only empty or atomic values permit
materialization. Opaque sequences, objects and arrays retain evaluation per reference without
inspecting their fields. Global-default proofs exclude constructors and stored composite reads,
including those reached through aliases, because their values may have escaped to a consumer.
Caller bindings can override even non-external global declarations in Brackit. Immutable metadata
records the global defaults used by each proof; the materializing expression checks for overrides
once when that binding evaluates and leaves an overridden source lazy. External declarations,
including ones with defaults, are never statically admitted. This is a purity check, with no saved
sequence or invalidation state. Resolved user functions that shadow JSON read functions and
implicit context arguments without a source proof are also excluded. A source that reads stored
JSON is materialized only with the stock `BasicJsonDBStore`; a custom provider keeps the generic
per-reference behavior because its document and field reads have no purity proof.

Single-reference bindings remain lazy. Admission currently targets lazy FLWOR (`PipeExpr`)
initializers; it does not add materialization to scalar lets or module-global declarations.
`sirix.optimizer.materializeLets=false` disables admission for fresh compilations. As with other
optimizer controls, use a fresh compile chain for A/B comparisons to avoid an already-cached plan.

## Behavioral and work evidence

The regression tests were added and executed before implementation. In two evaluations of the
same compiled Q3 over 100 stored rows, the baseline read **600** source-array items, and the
fixed executable plan reads **200**: exactly one 100-row scan per evaluation. The budget test
also executes the disabled-stage baseline and requires its 600 reads, proving the counting seam
observes all three references. The Q3 plan must carry exactly one materialization marker; the
baseline carries none.

`LetMaterializationWorkBudgetTest` additionally checks empty, singleton, 10,000-item and partially
consumed results against the disabled-stage plan, nested bindings, and correlated rebinding for
three outer tuples. `LetMaterializationTest` checks shadowing, single/unused bindings, unproven
calls, effectful lazy dependencies caller-dependent function parameters, global overrides, external defaults, shadowed functions,
implicit context arguments and positional `allowing empty` bindings. `MaterializeExprTest`
checks repeated binding evaluations, repeated result consumption, cursor closure, failure recovery,
and rejection of updating expressions. Custom-provider reads retain their original per-reference
execution; the stock-provider Q3 budget still requires exactly one scan per binding. The initial targeted run passed **18 tests**. After the additional purity cases, the final targeted
run passed **27 tests**, including a custom-provider regression that first failed with one lookup
instead of the required two.

## SH1 Q3 measurement

Measured on 2026-10-06 in this isolated worktree, using a private copy of the SH1 t100k
`t100k-hot-44fc2f4afe01-nodiffs` database. Both variants use the normative `BitemporalQueries`
Q3 and the same production compile chain/runtime; only the materialization stage is toggled.
Each variant has five warmups and nine measured executions, with alternating order. Compilation
and canonicalization are outside the measured interval; complete query serialization is inside it.
Every execution, including warmups, is canonicalized and byte-compared to the independent SH1
oracle (`60121\t60321\t3\n`). No wall-clock threshold is asserted in the automated tests.

| Variant | Warm median | Q3 source scans per binding |
|---|---:|---:|
| Materialization disabled | 228.799 ms | 3 |
| Materialization enabled | 74.553 ms | 1 |

The measured reduction is **3.07x**. These are contemporary A/B figures with the already-landed
cheap-first predicate ordering and current dependencies, rather than a comparison against the
older profiling report's 1,318 ms baseline. Timings on this shared laptop are evidence, not a CI
latency guarantee.

The [probe source](../bundles/sirix-query/bench/bitemporal/evidence/let-materialization-2026-10-06/Q3Probe.java.txt)
and [samples](../bundles/sirix-query/bench/bitemporal/evidence/let-materialization-2026-10-06/q3-samples.tsv)
are retained with the benchmark evidence. The oracle TSV SHA-256 is
`2b33db4275bb42f191ca5d8096a3be65536d2dff0d34e5b49cc33a4e553eaefd`.
Source metadata, regression XML and logs are under `build/let-materialize/`. The verification plan there records the initially empty private Maven
repository, `build/let-materialize/m2-private`, used for every Gradle command. Published Brackit
snapshot `1.0-alpha10-20261006.152144-93` was resolved, avoiding the stale local `~/.m2` snapshot.
All Gradle and benchmark JVMs run under the prescribed memory/lock limiter. Query test forks use
`-PtestHeapMin=256m -PtestHeapMax=2g`.

## Suite validation

The full sirix-core suite reports **13,156 tests**, 78 skipped, with zero failures or errors.
The fresh full sirix-query suite on the final unchanged implementation reports **3,018 tests**,
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
