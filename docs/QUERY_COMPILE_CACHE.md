# Query compilation caches

Reuse a `SirixCompileChain` for queries over the same stores. Repeated `new Query(chain, text)`
requests reuse translated code and skip parsing, analysis, optimization and translation. Each
chain keeps at most 128 translated modules and 128 optimized target trees. Query texts longer
than 65,536 characters compile normally without entering either shared cache.

The translated-module key contains exact query text, the process-wide schema version, the bound
JSON document hint (database, resource and revision), and morsel mode. Stores, session binding,
parallel mode and ordering belong to the chain and are never shared across chains. Scalar external
variables remain runtime bindings. Caller-registered vectorized executors, imported modules,
library modules and debug compilations bypass translated caching. Imported code cannot be
versioned by the importing query's text alone.

The optimizer cache includes the full parsed query and prolog, the individual target tree, target
slot, options, disabled stages and schema version. Parser comments do not change this identity.
`AST.toString()` is only a root label and is not a safe query key. Cached trees are detached before
translation and ordinary static-context references are rebound to the current analysis. Targets
containing nested static contexts or inline functions bypass this cache; declared functions and
imports also bypass sharing optimized trees. Their translated modules can still be reused when
self-contained.

Core index catalog mutations advance `IndexSchemaVersion`, including creates, drops and catalog
restoration. Revision publication also advances it because document paths, resource contents or a
reverted catalog can change routing. This is conservative: an unrelated resource commit also
invalidates a chain's caches. The next compilation releases the old generation; close releases
both caches. Compilation that overlaps a version change is never published into the cache. A
retained query eligible for translated caching checks the version at evaluation entry and recompiles when necessary, so removed
indexes are not reused by an already prepared query.

Compiler misses delegate to Brackit's original compiler pipeline. Bypassed compilations retain
syntax diagnostics without retaining their translated module or caller-owned executor graph.
Compiler misses are serialized within a chain to protect the resolver and compiler diagnostics.
Execution does not take that monitor. Brackit's `QueryExecution` owns declared-variable values
for each execution, operators create their own cursors, and Sirix descendant path matches are
worker-local and keyed by database, resource, revision and field. Concurrent callers use separate
query contexts and transactions. No context, result sequence or transaction is held in either
compilation cache.

`-Dsirix.query.compileWorkDiag=true` enables four phase counters in `CompileWorkStats`; it is off
by default. `CompiledQueryWorkBudgetTest` captures a nonzero cold compilation and requires zero
in all four phases for the second execution. `CompiledQueryCacheTest` covers bindings, core DDL,
retained queries, concurrent reuse, imported code, diagnostics and bounded eviction. Run them
through the commands in [VERIFICATION.md](VERIFICATION.md).
