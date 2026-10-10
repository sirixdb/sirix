# Query compilation caches

Reuse a `SirixCompileChain` for queries over the same stores. Repeated `new Query(chain, text)`
requests reuse optimized plans and skip parsing, analysis and optimization. Each execution
translates fresh executable code. Each chain keeps at most 128 query plans and 128 optimized target trees. Query texts longer
than 65,536 characters compile normally without entering either shared cache.

The query-plan key contains exact query text, the process-wide schema version, the bound
JSON document hint (database, resource and revision), and morsel mode. Stores, session binding,
parallel mode and ordering belong to the chain and are never shared across chains. Scalar external
variables remain runtime bindings. Caller-registered vectorized executors, imported modules,
library modules, user functions, explicit context-item/decimal-format declarations, nested contexts
and debug compilations bypass query-plan caching. Imported code cannot be
versioned by the importing query's text alone.

The optimizer cache includes the full parsed query and prolog, the individual target tree, target
slot, options, disabled stages and schema version. Its target keys share the owning query fingerprint
instead of retaining a copy of that potentially large string for every declaration and body.
Parser comments do not change this identity.
`AST.toString()` is only a root label and is not a safe query key. Cached trees are detached before
translation and ordinary static-context references are rebound to the current analysis. Targets
containing nested static contexts or inline functions bypass this cache; declared functions and
imports also bypass sharing optimized trees. They compile through the original pipeline.

Core index catalog mutations advance `IndexSchemaVersion`, including creates, drops and catalog
restoration. Revision publication also advances it because document paths, resource contents or a
reverted catalog can change routing. This is conservative: an unrelated resource commit also
invalidates a chain's caches. The next compilation releases the old generation; close releases
both caches. Compilation that overlaps a version change is never published into the cache. A
retained query checks schema and binding fences in `Module.getBody()`, before `QueryExecution`
starts. The initially translated body is claimed once; subsequent executions translate a fresh
body from the plan, recompiling when the fences change. Expression objects, declaration units
and runtime buffers are never reused by the cache. The returned body has no evaluation wrapper.

Compiler misses delegate to Brackit's original compiler pipeline. Bypassed compilations retain
syntax diagnostics without retaining their translated module or caller-owned executor graph.
Compilation and plan translation are serialized within a chain to protect the resolver and compiler diagnostics.
Execution does not take that monitor. Brackit's `QueryExecution` owns declared-variable values
for each execution, operators create their own cursors, and Sirix descendant path matches are
worker-local and keyed by database, resource, revision and field. Plans contain detached ASTs,
immutable settings and declaration metadata; they hold no translated module or executable units. Concurrent callers use separate
query contexts and transactions. No execution context, result sequence or transaction is held in either
compilation cache.

`-Dsirix.query.compileWorkDiag=true` enables phase counters and actual recursive translator-dispatch counts in `CompileWorkStats`; it is off
by default. `CompiledQueryWorkBudgetTest` captures a nonzero cold compilation and requires zero parse, analysis and
optimization work on repeats, exactly one translation phase, and a fixed expression-dispatch
count across both new and retained Query executions. This PR-specific contract was revised
to fresh translation to prevent execution-state sharing; no existing work budget is widened. `CompiledQueryCacheTest` covers bindings, core DDL,
retained queries, concurrent reuse, imported code, diagnostics and bounded eviction. Run them
through the commands in [VERIFICATION.md](VERIFICATION.md).
