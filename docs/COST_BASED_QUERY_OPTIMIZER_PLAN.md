# Cost-Based Query Optimization Plan for SirixDB/Brackit

## Inspired by Weiner, Mathis & Härder (TU Kaiserslautern) — Mapped to JSON

> **References**:
> - A. M. Weiner, C. Mathis, T. Härder: "Towards Cost-based Query Optimization in Native XML Database Systems" (CEUR-WS, 2008)
> - A. M. Weiner: "Cost-Based XQuery Optimization in Native XML Database Systems — Concepts, Implementation, and Empirical Evaluation" (Ph.D. thesis, TU Kaiserslautern)
> - R. F. de Moraes Filho, T. Härder: "Statistics for Cost-Based XML Query Optimization" (2006)
> - T. Neumann, B. Radke: "Adaptive Optimization of Very Large Join Queries" (SIGMOD 2018)
> - G. Moerkotte, T. Neumann: "Dynamic Programming Strikes Back" (DPhyp, SIGMOD 2008)
> - G. Moerkotte, P. Fender, M. Eich: "On the Correct and Complete Enumeration of the Core Search Space" (SIGMOD 2013)

---

## 1. Executive Summary

This plan details a complete cost-based query optimization framework for SirixDB's JSON/JSONiq query engine, built on top of the existing Brackit compiler infrastructure. It adapts the XQGM (XML Query Graph Model) from Weiner et al. into a **JQGM (JSON Query Graph Model)** — a new internal representation that mediates between JSONiq language-level expressions and physical algebra operators.

The design follows HFT-style high-performance principles: primitive types, minimal allocations, object pooling, and efficient data structures. It integrates with SirixDB's existing PathSummary statistics, index infrastructure, and Brackit's stage-based optimizer pipeline.

### Key Innovations from Weiner et al. Adapted to JSON

| Weiner (XML/XQGM) | SirixDB Adaptation (JSON/JQGM) |
|---|---|
| Tuple Sequence Operators (TOs) for XML axes | JSON Tuple Operators (JTOs) for object deref, array access, descendant paths |
| n-way structural JOIN (child/descendant axes) | n-way JSON path JOIN (deref chains, recursive descent) |
| NODE ACCESS / DOCUMENT ACCESS operators | OBJECT ACCESS / ARRAY ACCESS / DOCUMENT ACCESS operators |
| 6 rewrite rules (join fusion, select fusion, commutativity, decomposition) | 6 adapted rewrite rules for JSON path patterns |
| Physical operator decision tree (StackTree, HTJ, EPPP, NLJ) | JSON physical operator decision tree (PathIndex scan, CAS B+-tree, NameIndex, NLJ) |
| Mesh data structure (Graefe/DeWitt Exodus) | Mesh for JSON equivalence classes |
| Profile hierarchy (Base Profiles at leaves, Intermediate Profiles at inner nodes) | JSON Profile hierarchy using PathSummary + IndexDef metadata |
| AXS-Stat structural statistics | JSON-Stat: per-path cardinality from PathNode.getReferences() |
| Two-phase optimization (heuristic initial QEP → cost-based refinement) | Same: Brackit heuristic stages → cost-based refinement stage |
| System R DP join ordering | **Adaptive DPhyp** (Neumann & Radke, SIGMOD 2018): DPhyp for small queries, linearized DP for medium, GOO-DP for large — same as HyPer/Umbra/DuckDB |

---

## 2. Architecture Overview

### 2.1 Current State

- **Brackit Optimizer**: 7-stage pipeline — Simplification → Pipelining → Reordering → JoinRecognition → Unnest → FinalizePipeline → Finalize
- **Walker Pattern**: AST transformations via `Walker`/`ScopeWalker` base classes with auto-restart on modification
- **SirixOptimizer**: Extends `TopDownOptimizer`, adds `IndexMatching` stage with `JsonCASStep`, `JsonPathStep`, `JsonObjectKeyNameStep`
- **Existing Cost Model**: `SimpleCostModel` in Brackit with tunable JSON-specific weights (`brackit.cost.deref.level=0.2`, `brackit.cost.deref.descendant=5.0`, `brackit.cost.array.access=0.1`)
- **Statistics Available**:
  - `PathNode.getReferences()` — count of actual nodes per path pattern
  - `PathSummaryReader.match()` / `getPCRsForPath()` — BitSet of matching path nodes
  - `NodeReadOnlyTrx.getDescendantCount()`, `getChildCount()` — per-node statistics
  - `IndexController` — index availability checks (`containsIndex`, `hasCASIndex`, `hasPathIndex`, `hasNameIndex`)
  - `IndexDef` — index definitions with commented-out `IndexStatistics` code (lines 158-161)

### 2.2 Target State (Weiner-inspired)

Following Weiner's optimizer architecture (Figure 10 in the paper), we introduce five components:

```
                    ┌──────────────────────────────────────────┐
                    │            Query Optimizer                │
                    │                                          │
  Query Graph ─────►│  Generic Pattern    ◄──► Cost Model      │
  (JQGM)           │  Matcher (GPM)           (statistics)    │
                    │       │                      │           │
                    │       ▼                      ▼           │
                    │  Rewriter ◄──────► Plan Generator        │
                    │                                          │
                    └──────────────┬───────────────────────────┘
                                   │
                                   ▼
                          Query Execution Plan (QEP)
```

1. **Generic Pattern Matcher (GPM)** — Rule-based pattern matching on the JQGM AST (extends Brackit's existing `Walker` infrastructure)
2. **Rewriter** — Applies the 6 adapted rewrite rules via walker stages
3. **Plan Generator** — Two-phase: heuristic initial QEP → cost-based refinement using the Mesh
4. **Cost Model** — Extended `CostModel` interface with JSON-specific cost formulas
5. **Statistics** — `StatisticsProvider` backed by PathSummary, with JSON-Stat (adapted from AXS-Stat)

### 2.3 Optimizer Pipeline Integration

```
Existing Brackit stages:
  Simplification → Pipelining → Reordering → JoinRecognition → Unnest

New stages inserted (via SirixOptimizer.addStageBeforeIndexMatching):
  ... → JoinRecognition →
    ┌─── NEW: JqgmRewriteStage (Rules 1-4: join fusion, select fusion, commutativity) ───┐
    │─── NEW: CostBasedStage (statistics collection, cardinality estimation, join ordering)│
    │─── NEW: IndexDecompositionStage (Rules 5-6: index-aware join decomposition) ─────────┘
  → Unnest → FinalizePipeline → Finalize → IndexMatching
```

---

## 3. The JSON Query Graph Model (JQGM)

### 3.1 Mapping XQGM Operators to JSON

Weiner's XQGM defines Tuple Sequence Operators (TOs) that consume and produce ordered tuple sequences. We adapt each for JSON:

#### JSON Tuple Operators (JTOs)

| XQGM Operator | JQGM Operator | JSON Semantics |
|---|---|---|
| **SELECT** | **SELECT** | Filter tuples by value predicates (`where $x.price > 40`), positional predicates (`$arr[[2]]`), aggregate functions (`count`, `sum`) |
| **JOIN** (n-way structural) | **JOIN** (n-way JSON path) | Evaluates n-way JSON path joins. Binary: two deref chains sharing a common root. Complex: multi-path pattern over nested objects. Semi-join, anti-join, left-outer-join subtypes |
| **NODE ACCESS** | **OBJECT ACCESS** | Access sequence of JSON object nodes by key name. Maps to `DerefExpr` in Brackit AST. Can be backed by NameIndex or PathIndex |
| — | **ARRAY ACCESS** | Access JSON array elements by position or unboxing (`$arr[]`, `$arr[[0]]`). Maps to `ArrayAccess`/`ArrayAccessor` in Brackit AST |
| **DOCUMENT ACCESS** | **DOCUMENT ACCESS** | Access document root via `jn:doc()` or `jn:collection()`. Always maps to document scan |
| **GROUP BY** | **GROUP BY** | `group by` clause — groups tuple sequences, evaluates group-wise aggregates |
| **UNION/INTERSECT/DIFF** | **UNION/INTERSECT/DIFF** | Set operations on tuple sequences |
| **SORT** | **SORT** | `order by` clause — sorting and duplicate elimination |

#### Generic Tuple Operator (GTO) Structure — Adapted for JSON

Weiner's GTO has a **Head** (projection spec) and **Body** (derivation via inner graph of tuple variables). We preserve this:

```
JSON Generic Tuple Operator (JGTO):
  ┌─────────────────────────────────────────────────┐
  │ Head                                             │
  │   Head Attributes (projection: which fields to   │
  │   emit to consuming operator)                    │
  │   Attribute Calculation (fn:distinct-doc-order    │
  │   equivalent for JSON ordering)                  │
  ├─────────────────────────────────────────────────┤
  │ Body                                             │
  │   Tuple Variables:                               │
  │     F-quantified (for $x in ...) — iterate       │
  │     L-quantified (let $y := ...) — bind once     │
  │   Edges:                                         │
  │     Definition edges → input TOs                 │
  │     Predicate edges → filter conditions          │
  │     Output edges → accessible to other TOs       │
  │   Predicates:                                    │
  │     Structural: .key, .[], .descendant-objects()  │
  │     Value-based: =, <, >, !=, contains()         │
  │     Positional: [[0]], [[1..3]]                  │
  └─────────────────────────────────────────────────┘
```

### 3.2 JSON Path Patterns (Analog to Twig Query Patterns)

Weiner defines **Twig Query Patterns (TQPs)** as tree `(V, E, λ, r)` for XML structural navigation. For JSON:

**Definition: JSON Path Pattern (JPP)**

A JSON Path Pattern is a tree `JPP = (V, E, λ, r)` where:
- `V` = set of vertices (JSON nodes: objects, arrays, values)
- `E ⊆ V × V` = edges
- `λ : E → {child-key, child-index, descendant-key, descendant-or-self}` mapping
- `r` = root of the pattern

Where:
- `child-key` = object field dereference (`$x.name`) — analogous to XML `child` axis
- `child-index` = array index access (`$x[[0]]`) — analogous to XML positional predicate
- `descendant-key` = recursive descent (`$x..name`) — analogous to XML `descendant` axis
- `descendant-or-self` = recursive descent including self

**Example mapping**:
```
XPath:  /site/closed_auctions/closed_auction/price/text()
JSONiq: $doc.site.closed_auctions[].price

XQGM TQP:            JQGM JPP:
  site                  site
  └─child─►               └─child-key─►
    closed_auctions         closed_auctions
    └─child─►                 └─child-index─► (array unbox)
      closed_auction            [*]
      └─child─►                   └─child-key─►
        price                       price
```

### 3.3 Evaluation Strategies for JSON

Weiner identifies two strategies — **node-at-a-time** (nested loops, XQuery Core) and **set-at-a-time** (structural joins). For JSON:

| Strategy | JSON Mapping | When Used |
|---|---|---|
| **Node-at-a-time** | Navigate: `moveTo()` → `getObjectKeyName()` → `moveToFirstChild()` per node | Small results, no index, deep nesting |
| **Set-at-a-time** | Index scan: PathIndex/CASIndex returns sorted node set, then structural join | Large results, index available |
| **Hybrid** | Start with index scan for selective path, then navigate for remaining path steps | Partial index coverage |

---

## 4. Rewrite Rules for JSON (Adapted from Weiner's 6 Rules)

Weiner's rewrite philosophy: *"Whenever possible, a query should be converted to a single join operator"* — enabling evaluation via holistic twig joins (HTJ) or indexes. We adapt all 6 rules for JSON path patterns.

### Rule 1: Fusion of Adjacent JSON Path Joins

**Purpose**: Fuse two adjacent binary join operators evaluating JSON deref chains (child-key or descendant-key) into a single n-way join.

```
IF  (t is Join with F-quantified tuple variable v)
  ∧ (v references b)
  ∧ (b is Join)
  ∧ (b and t only contain structural predicates evaluating
     child-key or descendant-key navigation)
THEN {
  CopyBody(b,t), CopyHead(b,t), CopyPredicateEdge(b,t),
  UpdatePredicateEdge(v), UpdateDefinitionEdge(v),
  Delete(b), UpdateHeadAttributes(t), UpdateAttributeCalculation(t)
}
```

**JSON Example** — Before (two binary joins):
```
Join₁: $doc.site → .closed_auctions
Join₂: result₁ → [].price
```
After fusion (one 3-way join):
```
Join: $doc.site → .closed_auctions → [].price
```

**Brackit AST**: Walk `Join` nodes connected by `ForBind` over `DerefExpr` chains. When two joins share a `DerefExpr` ancestry, fuse into a single `Join` node with combined predicate edges.

### Rule 2: Fusion of Select and JSON Path Join

**Purpose**: Merge a Select (value predicate) into an adjacent Join when the Select filters the join's input.

```
IF  (t is Join with F-quantified tuple variable v)
  ∧ (v references b)
  ∧ (b is Select)
  ∧ (b contains no L-quantified tuple variable)
THEN {
  CopyBody(b,t), CopyHead(b,t), CopyPredicateEdge(b,t),
  UpdatePredicateEdge(v), UpdateDefinitionEdge(v), Delete(b)
}
```

**JSON Example**: Merge `where $x.price > 40` into the join evaluating `.closed_auction → .price`, enabling the value predicate to be pushed down into a CAS index lookup.

### Rule 3: Fusion of Select and Object/Array Access

**Purpose**: Merge a Select into an adjacent Object Access or Array Access operator, enabling direct mapping to index access.

```
IF  (t is Select with F-quantified tuple variable v)
  ∧ (v references b)
  ∧ (b is ObjectAccess or ArrayAccess)
THEN {
  CopyBody(t,b), CopyHead(t,b), CopyPredicateEdge(t,b),
  UpdatePredicateEdge(v), CopyOutputEdge(t,b), Delete(t)
}
```

**JSON Example**: `for $x in $doc.items[] where $x.category eq "books"` — the `Select(category eq "books")` is fused into the `ObjectAccess(items[])`, which can now be directly mapped to a CAS index on `items[].category`.

### Rule 4: Commutativity of Binary JSON Path Joins

**Purpose**: Allow swapping left/right join partners to extend the search space for join reordering.

```
IF  (t0 is binary Join)
  ∧ (t0 contains tuple variables v1 and v2)
  ∧ (v1 and v2 are connected to a structural predicate p)
  ∧ (p does not evaluate array-index axis — array order is directional)
  ∧ (v1 references TO t1) ∧ (v2 references TO t2)
THEN {
  ExchangeRef(v1:t1, v2:t1), ExchangeRef(v2:t2, v1:t2),
  ReversePredicate(p), UpdateOutputEdge(v1), UpdateOutputEdge(v2)
}
```

**JSON note**: Commutativity holds for `child-key` (forward: `.key`, reverse: parent), `descendant-key` (forward: `..key`, reverse: ancestor), but **NOT** for `child-index` (array position is inherently directional — there is no "parent array slot" reverse axis).

### Rule 5: Complex Join Decomposition (Index-Aware)

**Purpose**: Split an n-way join when a path/CAS index covers a subtree of the JSON path pattern.

```
IF  (t is n-way Join)
  ∧ (t contains one JSON path that can be answered using an available index)
  ∧ (t contains tuple variables v0, v1, and v2)
  ∧ (v0 is the JPP's root node)
  ∧ (all paths from v0 over v1 can be answered by no index)
  ∧ (at least one index can answer a path from v0 over v2)
THEN {
  CreateJoinOp(tb), MovePath(v0, v2, t, tb),
  UpdateHead(tb), InsertDefEdge(v0, tb)
}
```

**JSON Example**: Given indexes `I₁(//closed_auctions/closed_auction)` and `I₂(//price[String])`:
- The path `.site.closed_auctions[].price` can be split: the `.price` subtree maps to `I₂` (CAS index), while `.site.closed_auctions[]` is evaluated by navigation or `I₁`

### Rule 6: Overlapping Index Decomposition

**Purpose**: When multiple indexes overlap at the JPP root, split the join into two new joins connected by a structural self-join (intersection).

```
IF  (t is n-way Join)
  ∧ (t describes JPP that can use at least two indexes)
  ∧ (t contains tuple variables v0, v1, and v2)
  ∧ (v0 forms the JPP root node)
  ∧ (v1 and v2 are child nodes of v0)
  ∧ (v1 and v2 are root nodes of subtrees covered by different indexes)
THEN {
  CreateJoinOp(tl), CreateJoinOp(tr),
  MoveSubtree(t, tl, v1), MoveSubtree(t, tr, v2),
  InsertTupleVar(t, v3), InsertTupleVar(t, v4),
  InsertPredicate(t, self, v3, v4),
  UpdateHead(t), UpdateHead(tl), UpdateHead(tr),
  InsertDefEdge(v3, tl), InsertDefEdge(v4, tr)
}
```

**JSON Example**: Query on `$doc.products` with predicates on both `.category` (NameIndex) and `.price > 100` (CAS Index). Rule 6 splits into two index scans intersected at the `.products[]` level.

---

## 5. Physical Operator Decision Tree for JSON

Weiner's decision tree (Figure 12) selects physical operators for binary structural joins. We adapt for JSON:

```
            Evaluation methods for JSON path joins
                          │
              ┌───────────┴───────────┐
         No positional           Has positional
         predicate               predicate ([[n]])
              │                       │
    ┌─────────┼──────────┐      ┌─────┴──────┐
  child-key  descendant  other  child-key/   other
  (deref)    -key               child-index
    │           │          │        │           │
    ▼           ▼          ▼        ▼           ▼
  PathIndex  PathIndex   NLJ    Sequential   NLJ
  Scan       Scan               Scan +
  (if avail) (if avail)         Filter
    │           │
    │     ┌─────┴─────┐
    │   CAS Index   No Index
    │   (if value     │
    │    predicate)   ▼
    │     │        StackTree-style
    │     ▼        descendant join
    │   CAS B+-tree   (via cursor
    │   range scan     navigation)
    │
    ├── Has NameIndex? → NameIndex scan
    ├── Has PathIndex? → PathIndex scan
    └── No index? → Sequential scan + filter
```

### Physical Operator Mapping

| JSON Pattern | Index Available | Physical Operator | Weiner Analog |
|---|---|---|---|
| `$x.key` (child-key, no predicate) | PathIndex | PathIndex B+-tree scan | Element Index scan |
| `$x.key` (child-key, value predicate) | CAS Index | CAS B+-tree range scan | CAS Index scan |
| `$x.key` (child-key, no index) | None | Sequential scan + key filter | StackTree (Al-Khalifa) |
| `$x..key` (descendant-key) | PathIndex (descendant) | PathIndex descendant scan | HTJ (TwigStack) |
| `$x..key` (descendant-key, no index) | None | Cursor-based recursive descent | Nested-Loops Join |
| `$x[[n]]` (array position) | None | Sequential scan + position filter | EPPP (Vagena) |
| `$x[]` (array unbox) | None | Sequential child iteration | Full scan |
| Multi-path JPP (n-way join) | Multiple indexes | Index intersection (Rule 6) | TwigStack (Bruno) |
| Multi-path JPP (n-way join) | No index | Nested navigation loops | Nested-Loops Join |

### Complex n-way Join Mapping

Following Weiner Section 4.3.3: Complex n-way joins created by Rule 1 fusion are mapped as follows:
- **If path/CAS index covers complete JPP** → single index access operator (best case)
- **If path/CAS index covers partial JPP** → Rule 5 decomposition: index part + navigational join
- **If no index** → cursor-based nested navigation (equivalent to TwigStack for JSON trees)

---

## 6. Statistics Framework (JSON-Stat, Adapted from AXS-Stat)

### 6.1 Moraes Filho/Härder's AXS-Stat → JSON-Stat

Moraes Filho & Härder propose **AXS-Stat** — per-element structural statistics with (path, count) pairs for ancestor (PathA) and descendant (PathD) axes. We adapt for JSON:

**JSON-Stat** maintains per-path cardinality using SirixDB's existing `PathSummary`:

| AXS-Stat Concept | JSON-Stat Mapping | SirixDB Source |
|---|---|---|
| PathA(e) — ancestor path counts | parentPathCardinality(path) | `PathNode.getLevel()` + parent traversal |
| PathD(e) — descendant path counts | descendantPathCardinality(path) | `PathNode.getReferences()` (already tracks this) |
| Element count per path | nodeCount(path) | `PathNode.getReferences()` (returns `int`, widened to `long` in accumulation) |
| Structural selectivity | pathSelectivity(path) | `references / totalNodeCount` |
| Value histogram (for predicates) | valueDistribution(path, type) | Future: histogram on CAS index B+-tree |

### 6.2 Profile Hierarchy (Weiner Section 4.4)

Weiner uses a **profile hierarchy** with Base Profiles (BPs) at access operators and Intermediate Profiles (IPs) at inner operators:

```
JQGM Query Graph              Profile Hierarchy
┌──────────┐                   ┌──────┐
│  SELECT   │◄────────────────►│  IP  │ (derived from children)
│ count()   │                  │      │
└─────┬─────┘                  └──┬───┘
      │                           │
┌─────┴─────┐                  ┌──┴───┐
│   JOIN    │◄────────────────►│  IP  │
│ 3-way     │                  │      │
└──┬──┬──┬──┘                  └┬──┬──┘
   │  │  │                      │  │
   ▼  ▼  ▼                     ▼  ▼
 OBJ OBJ OBJ               BP  BP  BP
 ACC ACC ACC          (physical stats)
```

**Base Profile (BP)** — at OBJECT ACCESS / ARRAY ACCESS leaf operators:
```java
record BaseProfile(
    long nodeCount,          // PathNode.getReferences()
    int pathLevel,           // PathNode.getLevel()
    int btreeHeight,         // index B+-tree height (if index exists)
    long leafPageCount,      // number of leaf pages in index
    long distinctValues,     // distinct value count (from CAS index)
    boolean hasIndex,        // whether index covers this access
    IndexType indexType      // PATH, CAS, NAME, or NONE
) {}
```

**Intermediate Profile (IP)** — at JOIN, SELECT, GROUP BY inner operators:
```java
record IntermediateProfile(
    long estimatedCardinality,   // propagated from children via cost formulas
    double estimatedSelectivity, // for SELECT operators
    double estimatedCost,        // I/O + CPU cost
    long estimatedDistinct       // estimated distinct output tuples
) {}
```

Base Profiles are populated directly from `PathSummaryReader` and `IndexController`. Intermediate Profiles are **derived** from their children's profiles using the cost model — analogous to DB2's `runstats` tool updating IPs.

### 6.3 Statistics Provider Interface

**Location**: `sirix-query/src/main/java/io/sirix/query/compiler/optimizer/stats/`

```java
public interface StatisticsProvider {
    /**
     * Get cardinality for a JSON path (number of nodes matching path pattern).
     * Uses PathSummaryReader.getPCRsForPath() + PathNode.getReferences().
     * Returns -1 if unknown.
     */
    long getPathCardinality(Path<QNm> path, String databaseName,
                            String resourceName, int revision);

    /**
     * Get total document size (number of nodes).
     * Uses NodeReadOnlyTrx.getDescendantCount() from document root.
     */
    long getTotalNodeCount(String databaseName, String resourceName, int revision);

    /**
     * Estimate selectivity for a value predicate on a path.
     * Returns value in [0.0, 1.0].
     * Default estimates when no histograms: EQ=0.01, RANGE=0.33, NE=0.99.
     * Future: histogram-based estimation from CAS index B+-tree.
     */
    double estimateValueSelectivity(Path<QNm> path, Atomic value, String comparator,
                                    String databaseName, String resourceName, int revision);

    /**
     * Get index info for a path — checks CAS, PATH, NAME indexes.
     * Returns IndexInfo with indexId, type, and availability.
     */
    IndexInfo getIndexInfo(Path<QNm> path, String databaseName,
                           String resourceName, int revision);

    /**
     * Build a BaseProfile for an access operator at the given path.
     * Populates nodeCount, pathLevel, btreeHeight, etc.
     */
    BaseProfile buildBaseProfile(Path<QNm> path, String databaseName,
                                 String resourceName, int revision);

    /**
     * Get ancestor path cardinalities (adapted from AXS-Stat PathA).
     * Returns cardinality of parent path for selectivity estimation
     * in descendant-key joins.
     */
    long getAncestorPathCardinality(Path<QNm> path, String databaseName,
                                    String resourceName, int revision);
}
```

### 6.4 Sirix Statistics Provider Implementation

```java
public final class SirixStatisticsProvider implements StatisticsProvider, AutoCloseable {

    private final JsonDBStore jsonStore;

    // Fastutil primitive cache — zero boxing on hot path
    private final Object2LongOpenHashMap<PathCacheKey> pathCardinalityCache;

    // Session cache: avoids opening/closing resource sessions on every statistics call.
    // Key = "dbName:resourceName", Value = cached ResourceSession.
    // Sessions are opened lazily and closed when the provider is closed or cleared.
    private final Object2ObjectOpenHashMap<String, ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx>>
        sessionCache;

    public SirixStatisticsProvider(JsonDBStore jsonStore) {
        this.jsonStore = jsonStore;
        this.pathCardinalityCache = new Object2LongOpenHashMap<>();
        this.pathCardinalityCache.defaultReturnValue(-1L);
        this.sessionCache = new Object2ObjectOpenHashMap<>();
    }

    /**
     * Get or create a cached resource session. Avoids the overhead of
     * beginResourceSession()/close() on every statistics call.
     * Does NOT cache null — transient failures will be retried on next call.
     */
    private ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> getSession(
            String databaseName, String resourceName) {
        final String key = databaseName + ":" + resourceName;
        ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> session = sessionCache.get(key);
        if (session != null) return session;

        try {
            final var jsonCollection = jsonStore.lookup(databaseName);
            session = jsonCollection.getDatabase().beginResourceSession(resourceName);
            sessionCache.put(key, session);
            return session;
        } catch (Exception e) {
            return null; // not cached — will retry on next call
        }
    }

    /**
     * Clear all caches and close cached sessions. Call between queries
     * to prevent stale statistics and resource leaks.
     */
    public void clearCaches() {
        pathCardinalityCache.clear();
        for (var session : sessionCache.values()) {
            if (session != null) {
                try { session.close(); } catch (Exception ignored) {}
            }
        }
        sessionCache.clear();
    }

    @Override
    public void close() {
        clearCaches();
    }

    @Override
    public long getPathCardinality(Path<QNm> path, String databaseName,
                                   String resourceName, int revision) {
        final var key = new PathCacheKey(path, databaseName, resourceName, revision);
        long cached = pathCardinalityCache.getLong(key);
        if (cached != -1L) {
            return cached;
        }

        try {
            final var resMgr = getSession(databaseName, resourceName);
            if (resMgr == null) return -1L;

            // PathSummary is lightweight — open/close per call is fine
            try (final var pathSummary = revision == -1
                     ? resMgr.openPathSummary()
                     : resMgr.openPathSummary(revision)) {

                final LongSet pcrs = pathSummary.getPCRsForPath(path);
                long totalReferences = 0L;

                for (long pcr : pcrs) {
                    final PathNode pathNode = pathSummary.getPathNodeForPathNodeKey(pcr);
                    if (pathNode != null) {
                        // Note: getReferences() returns int, safely widened to long.
                        // Accumulation in long prevents overflow when summing across PCRs.
                        totalReferences += pathNode.getReferences();
                    }
                }

                pathCardinalityCache.put(key, totalReferences);
                return totalReferences;
            }
        } catch (Exception e) {
            return -1L;
        }
    }

    @Override
    public long getTotalNodeCount(String databaseName, String resourceName, int revision) {
        try {
            final var resMgr = getSession(databaseName, resourceName);
            if (resMgr == null) return -1L;

            try (final var rtx = revision == -1
                     ? resMgr.beginNodeReadOnlyTrx()
                     : resMgr.beginNodeReadOnlyTrx(revision)) {
                rtx.moveToDocumentRoot();
                return rtx.getDescendantCount();
            }
        } catch (Exception e) {
            return -1L;
        }
    }

    @Override
    public double estimateValueSelectivity(Path<QNm> path, Atomic value, String comparator,
                                           String databaseName, String resourceName, int revision) {
        // Phase 1: default selectivity estimates (no histograms yet)
        // Phase 2+: histogram-based from CAS index B+-tree leaf scanning
        return switch (comparator) {
            case "ValueCompEQ", "GeneralCompEQ" -> 0.01;     // 1%
            case "ValueCompLT", "ValueCompLE",
                 "ValueCompGT", "ValueCompGE",
                 "GeneralCompLT", "GeneralCompLE",
                 "GeneralCompGT", "GeneralCompGE" -> 0.33;   // 33%
            case "ValueCompNE", "GeneralCompNE" -> 0.99;     // 99%
            default -> 0.5;
        };
    }

    @Override
    public IndexInfo getIndexInfo(Path<QNm> path, String databaseName,
                                  String resourceName, int revision) {
        try {
            final var resMgr = getSession(databaseName, resourceName);
            if (resMgr == null) return IndexInfo.NO_INDEX;

            final var indexController = revision == -1
                ? resMgr.getRtxIndexController(resMgr.getMostRecentRevisionNumber())
                : resMgr.getRtxIndexController(revision);

            final var indexes = indexController.getIndexes();

            // CAS index first (most useful for value predicates)
            for (IndexDef indexDef : indexes.getIndexDefs()) {
                if (indexDef.isCasIndex()) {
                    for (Path<QNm> indexedPath : indexDef.getPaths()) {
                        if (indexedPath.matches(path)) {
                            return new IndexInfo(indexDef.getID(), IndexType.CAS, true);
                        }
                    }
                }
            }

            // Path index
            for (IndexDef indexDef : indexes.getIndexDefs()) {
                if (indexDef.isPathIndex()) {
                    for (Path<QNm> indexedPath : indexDef.getPaths()) {
                        if (indexedPath.matches(path)) {
                            return new IndexInfo(indexDef.getID(), IndexType.PATH, true);
                        }
                    }
                }
            }

            // Name index — must also verify it covers the requested path's leaf name.
            // A NameIndex on "price" should only match paths ending in "price".
            if (path.getLength() > 0) {
                QNm leafName = path.getComponent(path.getLength() - 1);
                for (IndexDef indexDef : indexes.getIndexDefs()) {
                    if (indexDef.isNameIndex()
                            && indexDef.getIncludedNames().contains(leafName)) {
                        return new IndexInfo(indexDef.getID(), IndexType.NAME, true);
                    }
                }
            }

            return IndexInfo.NO_INDEX;
        } catch (Exception e) {
            return IndexInfo.NO_INDEX;
        }
    }

    @Override
    public BaseProfile buildBaseProfile(Path<QNm> path, String databaseName,
                                        String resourceName, int revision) {
        final long nodeCount = getPathCardinality(path, databaseName, resourceName, revision);
        final IndexInfo indexInfo = getIndexInfo(path, databaseName, resourceName, revision);

        // Get path level from PathSummary
        // Note: if multiple PCRs match, we take the minimum level (shallowest path),
        // which gives the most conservative cost estimate for navigation depth.
        int pathLevel = Integer.MAX_VALUE;
        int btreeHeight = -1;
        long leafPageCount = -1;
        long distinctValues = -1;

        try {
            final var resMgr = getSession(databaseName, resourceName);
            if (resMgr != null) {
                try (final var pathSummary = revision == -1
                         ? resMgr.openPathSummary()
                         : resMgr.openPathSummary(revision)) {

                    final LongSet pcrs = pathSummary.getPCRsForPath(path);
                    for (long pcr : pcrs) {
                        final PathNode pathNode = pathSummary.getPathNodeForPathNodeKey(pcr);
                        if (pathNode != null) {
                            pathLevel = Math.min(pathLevel, pathNode.getLevel());
                        }
                    }
                }
            }
        } catch (Exception e) {
            // defaults remain
        }

        if (pathLevel == Integer.MAX_VALUE) pathLevel = -1; // no match found

        return new BaseProfile(nodeCount, pathLevel, btreeHeight, leafPageCount,
                               distinctValues, indexInfo.exists(), indexInfo.type());
    }

    @Override
    public long getAncestorPathCardinality(Path<QNm> path, String databaseName,
                                           String resourceName, int revision) {
        // Get parent path cardinality (AXS-Stat PathA analog)
        if (path.getLength() <= 1) {
            return 1L; // root has no ancestor
        }
        final Path<QNm> parentPath = path.leading();
        return getPathCardinality(parentPath, databaseName, resourceName, revision);
    }
}

record IndexInfo(int indexId, IndexType type, boolean exists) {
    static final IndexInfo NO_INDEX = new IndexInfo(-1, IndexType.NONE, false);
}
// Note: IndexType enum should include NONE alongside PATH, CAS, NAME, PATH_SUMMARY

record PathCacheKey(Path<QNm> path, String databaseName, String resourceName, int revision) {}
```

---

## 7. Cost Model

### 7.1 Enhanced Cost Model Interface

Extending Brackit's existing `CostModel` interface with JSON-specific cost formulas based on Weiner's profile-based cost estimation:

```java
public interface CostModel {
    /**
     * Estimate cost for an operator given input cardinality and context.
     * Returns cost as primitive double (zero boxing).
     * Cost = I/O cost + CPU cost (Weiner Section 4.4)
     */
    double estimateCost(int operatorType, long inputCardinality, OperatorContext ctx);

    /**
     * Compare two costs. Returns negative if cost1 < cost2.
     */
    int compareCosts(double cost1, double cost2);

    /**
     * Estimate cost for a specific physical operator choice.
     * Used during plan generation to compare alternative physical operators
     * (Weiner Figure 12 decision tree).
     */
    double estimatePhysicalOperatorCost(PhysicalOperator op, BaseProfile inputProfile);
}
```

### 7.2 Extended Operator Context

```java
public final class OperatorContext {
    // Primitive fields only — zero boxing on hot path
    public long inputCardinality;
    public long outputCardinality;
    public double selectivity;
    public int indexId;          // -1 if no index
    public int pathNodeKey;      // path summary key
    public boolean isIndexScan;
    public boolean isSequentialScan;

    // New: Weiner profile hierarchy fields
    public int btreeHeight;      // index B+-tree height (BP)
    public long leafPageCount;   // number of leaf pages (BP)
    public long distinctValues;  // distinct value count (BP)
    public int pathLevel;        // depth in JSON tree (BP)
    public double ioWeight;      // I/O cost weight
    public double cpuWeight;     // CPU cost weight

    // ThreadLocal pool with aliasing guard.
    // WARNING: only ONE OperatorContext may be alive per thread at a time.
    // Acquiring a second before releasing the first will reset the first.
    // Capture all needed values into local variables before acquiring again.
    private static final ThreadLocal<OperatorContext[]> POOL =
        ThreadLocal.withInitial(() -> new OperatorContext[]{ new OperatorContext(), new OperatorContext() });
    private static final ThreadLocal<int[]> POOL_IDX = ThreadLocal.withInitial(() -> new int[]{0});

    public static OperatorContext acquire() {
        int[] idx = POOL_IDX.get();
        OperatorContext ctx = POOL.get()[idx[0] & 1]; // alternate between two instances
        idx[0]++;
        return ctx.reset();
    }

    public OperatorContext reset() {
        inputCardinality = 0L;
        outputCardinality = 0L;
        selectivity = 1.0;
        indexId = -1;
        pathNodeKey = -1;
        isIndexScan = false;
        isSequentialScan = false;
        btreeHeight = 0;
        leafPageCount = 0;
        distinctValues = 0;
        pathLevel = 0;
        ioWeight = 1.0;
        cpuWeight = 1.0;
        return this;
    }
}
```

### 7.3 JSON-Aware Cost Model Implementation

```java
public final class JsonCostModel implements CostModel {

    // Cost weights — configurable via system properties
    // JSON-specific weights extend Brackit's existing tunable parameters
    private static final double SEQ_SCAN_COST_PER_TUPLE = 1.0;
    private static final double INDEX_LOOKUP_OVERHEAD = 10.0;   // B+-tree root-to-leaf
    private static final double INDEX_SCAN_COST_PER_TUPLE = 0.1;
    private static final double HASH_JOIN_BUILD_COST = 1.5;
    private static final double HASH_JOIN_PROBE_COST = 0.5;
    private static final double SORT_COST_FACTOR = 1.5;         // n * log(n)
    private static final double DEREF_COST = 0.2;               // object key dereference
    private static final double ARRAY_ACCESS_COST = 0.1;        // array index access
    private static final double ARRAY_UNBOX_COST = 0.05;        // array iteration
    private static final double DESCENDANT_COST_MULTIPLIER = 5.0; // recursive descent

    // Moraes Filho/Härder: I/O cost = pages_read * page_io_cost
    private static final double PAGE_IO_COST = 10.0;
    private static final double CPU_COMPARISON_COST = 0.001;

    @Override
    public double estimateCost(int operatorType, long inputCardinality, OperatorContext ctx) {
        return switch (operatorType) {
            case XQ.ForBind -> estimateAccessCost(inputCardinality, ctx);
            case XQ.Selection -> inputCardinality * CPU_COMPARISON_COST; // every row checked
            case XQ.Join -> estimateJoinCost(inputCardinality, ctx);
            case XQ.OrderBy -> inputCardinality * SORT_COST_FACTOR
                               * Math.log(inputCardinality + 1) * CPU_COMPARISON_COST;
            case XQ.GroupBy -> inputCardinality * HASH_JOIN_BUILD_COST * CPU_COMPARISON_COST;
            default -> inputCardinality;
        };
    }

    // Approximate entries per leaf page (KVL page is 64KB, ~100-200 entries typical)
    private static final double ENTRIES_PER_PAGE = 100.0;

    private double estimateAccessCost(long inputCardinality, OperatorContext ctx) {
        if (ctx.isIndexScan) {
            // I/O cost: B+-tree traversal + leaf page reads
            double ioCost = ctx.btreeHeight * PAGE_IO_COST
                          + (inputCardinality / ENTRIES_PER_PAGE) * PAGE_IO_COST;
            double cpuCost = inputCardinality * INDEX_SCAN_COST_PER_TUPLE;
            return INDEX_LOOKUP_OVERHEAD + ioCost * ctx.ioWeight + cpuCost * ctx.cpuWeight;
        } else {
            // Sequential scan: derive page count from cardinality if not explicitly set
            long pages = ctx.leafPageCount > 0
                ? ctx.leafPageCount
                : Math.max(1L, (long)(inputCardinality / ENTRIES_PER_PAGE));
            double ioCost = pages * PAGE_IO_COST;
            double cpuCost = inputCardinality * SEQ_SCAN_COST_PER_TUPLE;
            return ioCost * ctx.ioWeight + cpuCost * ctx.cpuWeight;
        }
    }

    private double estimateJoinCost(long probeCardinality, OperatorContext ctx) {
        // Hash join: build phase (from ctx.inputCardinality = build side)
        //          + probe phase (from probeCardinality = probe side)
        double buildCost = ctx.inputCardinality * HASH_JOIN_BUILD_COST;
        double probeCost = probeCardinality * HASH_JOIN_PROBE_COST;
        return buildCost + probeCost;
    }

    @Override
    public double estimatePhysicalOperatorCost(PhysicalOperator op, BaseProfile profile) {
        // Guard against unknown (-1) values from BaseProfile. Unknown fields default
        // to conservative estimates to avoid negative costs winning comparisons.
        final long nc = Math.max(0L, profile.nodeCount());
        final long lpc = Math.max(1L, profile.leafPageCount());
        final int bth = Math.max(0, profile.btreeHeight());
        final int pl = Math.max(1, profile.pathLevel());

        return switch (op) {
            case PATH_INDEX_SCAN -> INDEX_LOOKUP_OVERHEAD
                + bth * PAGE_IO_COST
                + nc * INDEX_SCAN_COST_PER_TUPLE;
            case CAS_INDEX_SCAN -> INDEX_LOOKUP_OVERHEAD
                + bth * PAGE_IO_COST
                + nc * INDEX_SCAN_COST_PER_TUPLE * 0.8; // CAS more selective
            case NAME_INDEX_SCAN -> INDEX_LOOKUP_OVERHEAD
                + nc * INDEX_SCAN_COST_PER_TUPLE;
            case SEQUENTIAL_SCAN -> lpc * PAGE_IO_COST
                + nc * SEQ_SCAN_COST_PER_TUPLE;
            case CURSOR_NAVIGATION -> nc * DEREF_COST * pl;
            case NESTED_LOOPS_JOIN ->
                // NLJ with single profile: assumes outer = inner = nodeCount.
                // For two-sided NLJ, use estimateNljCost(outerProfile, innerProfile) below.
                nc * nc * CPU_COMPARISON_COST;
        };
    }

    /**
     * NLJ cost with distinct outer and inner profiles.
     * Cost = |outer| × (inner scan cost per probe) + |outer| × CPU per comparison.
     */
    public double estimateNljCost(BaseProfile outer, BaseProfile inner) {
        double innerScanCost = inner.hasIndex()
            ? INDEX_LOOKUP_OVERHEAD + inner.btreeHeight() * PAGE_IO_COST
            : inner.nodeCount() * DEREF_COST;
        return outer.nodeCount() * (innerScanCost + inner.nodeCount() * CPU_COMPARISON_COST);
    }

    @Override
    public int compareCosts(double cost1, double cost2) {
        return Double.compare(cost1, cost2);
    }
}

enum PhysicalOperator {
    PATH_INDEX_SCAN,
    CAS_INDEX_SCAN,
    NAME_INDEX_SCAN,
    SEQUENTIAL_SCAN,
    CURSOR_NAVIGATION,
    NESTED_LOOPS_JOIN
}
```

---

## 8. Selectivity & Cardinality Estimation

### 8.1 Selectivity Estimator

```java
public final class SelectivityEstimator {

    private final StatisticsProvider statsProvider;

    // Default selectivity values (primitives, no boxing)
    private static final double DEFAULT_EQUALITY_SELECTIVITY = 0.01;
    private static final double DEFAULT_RANGE_SELECTIVITY = 0.33;
    private static final double DEFAULT_LIKE_SELECTIVITY = 0.25;
    private static final double DEFAULT_AND_DAMPING = 0.5;   // independence assumption

    public SelectivityEstimator(StatisticsProvider statsProvider) {
        this.statsProvider = statsProvider;
    }

    public double estimateSelectivity(AST predicate, RevisionData revisionData) {
        return switch (predicate.getType()) {
            case XQ.ComparisonExpr -> estimateComparisonSelectivity(predicate, revisionData);
            case XQ.AndExpr -> estimateAndSelectivity(predicate, revisionData);
            case XQ.OrExpr -> estimateOrSelectivity(predicate, revisionData);
            case XQ.NotExpr -> 1.0 - estimateSelectivity(predicate.getChild(0), revisionData);
            default -> 0.5;
        };
    }

    private double estimateComparisonSelectivity(AST comparison, RevisionData revisionData) {
        final AST compOp = comparison.getChild(0);
        final String comparator = compOp.getStringValue();

        // Extract path and constant value
        Path<QNm> path = extractPath(comparison.getChild(1));
        Atomic value = extractConstant(comparison.getChild(2));
        if (path == null) {
            path = extractPath(comparison.getChild(2));
            value = extractConstant(comparison.getChild(1));
        }

        if (path != null && value != null) {
            return statsProvider.estimateValueSelectivity(
                path, value, comparator,
                revisionData.databaseName(), revisionData.resourceName(),
                revisionData.revision());
        }

        // Fallback to defaults
        return switch (comparator) {
            case "ValueCompEQ", "GeneralCompEQ" -> DEFAULT_EQUALITY_SELECTIVITY;
            case "ValueCompLT", "ValueCompLE", "ValueCompGT", "ValueCompGE",
                 "GeneralCompLT", "GeneralCompLE", "GeneralCompGT", "GeneralCompGE"
                 -> DEFAULT_RANGE_SELECTIVITY;
            default -> 0.5;
        };
    }

    private double estimateAndSelectivity(AST andExpr, RevisionData revisionData) {
        // SQL Server exponential backoff: sort by selectivity ascending,
        // then sel[0] * sel[1]^(1/2) * sel[2]^(1/3) * ...
        // This avoids the independence assumption's systematic underestimation.
        final int childCount = andExpr.getChildCount();
        final double[] childSels = new double[childCount];
        for (int i = 0; i < childCount; i++) {
            childSels[i] = estimateSelectivity(andExpr.getChild(i), revisionData);
        }
        java.util.Arrays.sort(childSels); // ascending: most selective first

        double selectivity = 1.0;
        for (int i = 0; i < childCount; i++) {
            selectivity *= Math.pow(childSels[i], 1.0 / (i + 1));
        }
        return Math.max(0.0001, selectivity);
    }

    private double estimateOrSelectivity(AST orExpr, RevisionData revisionData) {
        // Inclusion-exclusion: P(A∪B) = P(A) + P(B) - P(A)·P(B)
        double selectivity = 0.0;
        for (int i = 0; i < orExpr.getChildCount(); i++) {
            double childSel = estimateSelectivity(orExpr.getChild(i), revisionData);
            selectivity = selectivity + childSel - (selectivity * childSel);
        }
        return Math.min(1.0, selectivity);
    }

    // Follows AbstractJsonPathWalker.traversePath pattern
    private Path<QNm> extractPath(AST expr) { /* ... */ return null; }
    private Atomic extractConstant(AST expr) { /* ... */ return null; }
}
```

### 8.2 Cardinality Estimator with Profile Propagation

Following Weiner's profile hierarchy — BPs at leaves propagate upward to IPs at inner operators:

```java
public final class CardinalityEstimator {

    private final StatisticsProvider statsProvider;
    private final SelectivityEstimator selectivityEstimator;

    public CardinalityEstimator(StatisticsProvider statsProvider,
                                SelectivityEstimator selectivityEstimator) {
        this.statsProvider = statsProvider;
        this.selectivityEstimator = selectivityEstimator;
    }

    private static final int MAX_PIPELINE_DEPTH = 500;

    public long estimatePipelineCardinality(AST pipelineNode, RevisionData revisionData) {
        return walkPipeline(pipelineNode, revisionData, 0);
    }

    private long walkPipeline(AST node, RevisionData revisionData, int depth) {
        if (depth > MAX_PIPELINE_DEPTH) return 1L; // safety: prevent stack overflow on cyclic/deep ASTs
        long cardinality = switch (node.getType()) {
            case XQ.Start -> estimateSourceCardinality(node, revisionData);
            case XQ.ForBind -> {
                long inputCard = walkPipeline(node.getLastChild(), revisionData, depth + 1);
                long bindingCard = estimateBindingCardinality(node, revisionData);
                yield inputCard * bindingCard;
            }
            case XQ.LetBind -> walkPipeline(node.getLastChild(), revisionData, depth + 1);
            case XQ.Selection -> {
                long inputCard = walkPipeline(node.getLastChild(), revisionData, depth + 1);
                double sel = selectivityEstimator.estimateSelectivity(
                    node.getChild(0), revisionData);
                yield Math.max(1L, (long)(inputCard * sel));
            }
            case XQ.Join -> {
                long leftCard = walkPipeline(node.getChild(0), revisionData, depth + 1);
                long rightCard = walkPipeline(node.getChild(1), revisionData, depth + 1);
                double joinSel = estimateJoinSelectivity(node, revisionData);
                yield Math.max(1L, (long)(leftCard * rightCard * joinSel));
            }
            case XQ.GroupBy -> {
                // Heuristic: GroupBy output cardinality ≈ √(input).
                // This is a standard DB2/PostgreSQL estimate when no column statistics
                // are available. With histograms (Phase 6), replace with
                // distinct-value count from the group-by key column.
                long inputCard = walkPipeline(node.getLastChild(), revisionData, depth + 1);
                yield Math.max(1L, (long) Math.sqrt(inputCard));
            }
            case XQ.OrderBy -> walkPipeline(node.getLastChild(), revisionData, depth + 1);
            case XQ.End -> 1L;
            default -> {
                if (node.getLastChild() != null) {
                    yield walkPipeline(node.getLastChild(), revisionData, depth + 1);
                }
                yield 1L;
            }
        };

        return cardinality;
    }

    private long estimateBindingCardinality(AST forBind, RevisionData revisionData) {
        AST bindingExpr = forBind.getChildCount() > 2
            ? forBind.getChild(2) : forBind.getChild(1);

        Path<QNm> path = extractPathFromExpr(bindingExpr);
        if (path != null) {
            long pathCard = statsProvider.getPathCardinality(
                path, revisionData.databaseName(),
                revisionData.resourceName(), revisionData.revision());
            if (pathCard > 0) return pathCard;
        }
        return 100L; // default estimate
    }

    /**
     * JSON-specific join selectivity using AXS-Stat-style
     * ancestor/descendant cardinality ratios.
     */
    private double estimateJoinSelectivity(AST join, RevisionData revisionData) {
        // For child-key join: sel = 1/avg_children
        // For descendant-key join: sel = 1/avg_descendants
        // Default: 0.1 (10%)
        return 0.1;
    }

    private long estimateSourceCardinality(AST start, RevisionData revisionData) {
        AST child = start.getLastChild();
        if (child != null && child.getType() == XQ.ForBind) {
            return estimateBindingCardinality(child, revisionData);
        }
        return 1L;
    }

    private Path<QNm> extractPathFromExpr(AST expr) { /* ... */ return null; }
}

record RevisionData(String databaseName, String resourceName, int revision) {}
```

---

## 9. Search Space Management — The Mesh

### 9.1 Mesh Data Structure (Graefe/DeWitt Exodus)

Weiner uses the **Mesh** (Section 4.2, Figure 11) to manage the search space of semantically equivalent QEPs. The Mesh groups alternative plans into **equivalence classes** — subtrees that produce the same result but differ in physical execution.

For JSON, the Mesh stores alternatives generated by:
- **Rewrite rules 1-6** producing equivalent JQGM subgraphs
- **Physical operator alternatives** (PathIndex scan vs sequential scan vs cursor navigation)
- **Join order alternatives** (from commutativity Rule 4 + DP enumeration)

```java
public final class Mesh {

    // Equivalence classes: each class groups semantically equivalent AST subtrees
    // Key = equivalence class ID, Value = list of alternative physical plans
    private final Int2ObjectOpenHashMap<EquivalenceClass> classes;
    private int nextClassId;

    // Child class references: map parent class → child class IDs.
    // Virtual edges avoid copying shared subtrees — alternatives reference
    // children by equivalence class ID rather than duplicating AST nodes.
    private final Int2ObjectOpenHashMap<int[]> childClassRefs;

    public Mesh(int estimatedSize) {
        this.classes = new Int2ObjectOpenHashMap<>(estimatedSize);
        this.childClassRefs = new Int2ObjectOpenHashMap<>(estimatedSize);
        this.nextClassId = 0;
    }

    /**
     * Create a new equivalence class with an initial plan.
     */
    public int createClass(AST initialPlan, double cost) {
        final int classId = nextClassId++;
        final var eqClass = new EquivalenceClass(classId);
        eqClass.addAlternative(initialPlan, cost);
        classes.put(classId, eqClass);
        return classId;
    }

    /**
     * Add an alternative plan to an existing equivalence class.
     * Called when a rewrite rule produces a new equivalent subtree.
     * Only physically new nodes are stored (virtual edges to shared children).
     */
    public void addAlternative(int classId, AST alternativePlan, double cost) {
        final var eqClass = classes.get(classId);
        if (eqClass != null) {
            eqClass.addAlternative(alternativePlan, cost);
        }
    }

    /**
     * Get the best (lowest-cost) plan from an equivalence class.
     */
    public AST getBestPlan(int classId) {
        final var eqClass = classes.get(classId);
        return eqClass != null ? eqClass.getBestPlan() : null;
    }

    /**
     * Set child equivalence class references for a parent class.
     * Enables virtual edges: instead of copying shared subtrees,
     * the plan references child classes by ID (Graefe/DeWitt Exodus, Figure 11).
     */
    public void setChildClasses(int parentClassId, int[] childClassIds) {
        childClassRefs.put(parentClassId, childClassIds);
    }

    /**
     * Get child class references. Returns null if no children registered.
     */
    public int[] getChildClasses(int parentClassId) {
        return childClassRefs.get(parentClassId);
    }

    /**
     * Update the mesh after a rewrite rule application (Figure 11).
     * The original graph and the rewritten graph are both stored,
     * connected by virtual edges to shared subtrees.
     */
    public void updateAfterRewrite(int originalClassId, AST rewrittenPlan, double newCost) {
        addAlternative(originalClassId, rewrittenPlan, newCost);
    }

    /**
     * Clear all state for reuse between queries.
     */
    public void clear() {
        classes.clear();
        childClassRefs.clear();
        nextClassId = 0;
    }
}

final class EquivalenceClass {
    final int classId;
    private final ObjectArrayList<PlanAlternative> alternatives;
    private int bestIndex;
    private double bestCost;

    EquivalenceClass(int classId) {
        this.classId = classId;
        this.alternatives = new ObjectArrayList<>(4); // typically few alternatives
        this.bestIndex = -1;
        this.bestCost = Double.MAX_VALUE;
    }

    void addAlternative(AST plan, double cost) {
        alternatives.add(new PlanAlternative(plan, cost));
        if (cost < bestCost) {
            bestCost = cost;
            bestIndex = alternatives.size() - 1;
        }
    }

    AST getBestPlan() {
        return bestIndex >= 0 ? alternatives.get(bestIndex).plan() : null;
    }
}

record PlanAlternative(AST plan, double cost) {}
```

---

## 10. Join Ordering — Adaptive DPhyp (Neumann & Radke, SIGMOD 2018)

### 10.1 Overview: Why Not System R DP?

The classic System R approach enumerates **all** 2^n subsets regardless of query graph structure.
DPhyp (Moerkotte & Neumann) instead enumerates only **connected subgraph complement pairs** —
it skips disconnected subsets that can never form a valid join tree. For chain queries this is O(n³)
instead of O(2^n). For star queries it's still exponential, but with much smaller constants.

The Neumann & Radke adaptive framework (used by HyPer, Umbra, and DuckDB) goes further:
it **selects the best algorithm based on query complexity**, gracefully scaling from 2 to 5,000+ relations.

### 10.2 The Adaptive Strategy

```
adaptive(Q = (V, E)):
  // Phase 1: Try exact optimization
  if |V| < 14 or countCC(Q, 10000) ≤ 10000:
    return DPhyp(Q)                                // exact optimal — graph-based DP

  // Phase 2: Search space linearization for medium queries
  if Q contains no hyper-edges:                     // no non-inner joins
    if |V| ≤ 100:
      return linearizedDP(Q)                        // IKKBZ linearization + chain DP
    return GOO_DP(Q, linearizedDP, 100, 10000)      // greedy + DP refinement

  // Phase 3: Hyper-edges (outer/semi/anti joins) — use DPhyp as inner DP
  return GOO_DP(Q, DPhyp, 10, 10000)
```

**Key insight from the paper**: The number of connected subgraphs (`countCC`) predicts DP table
size and thus optimization time. Counting is much cheaper than actual DP (no cost evaluation),
so we can decide the strategy in microseconds.

### 10.3 Component Algorithms

#### DPhyp — Graph-Based DP with Connected Subgraph Complement Pairs

DPhyp enumerates connected subgraph pairs (S₁, S₂) where S₁ ∪ S₂ forms a connected
subgraph and there exists a join edge between S₁ and S₂. This avoids generating
disconnected (invalid) subsets entirely.

```
DPhyp(Q = (V, E)):
  // Initialize: single-relation base cases
  for each v ∈ V:
    dpTable[{v}] = (cost=0, card=baseCardinality(v), plan=v)

  // Enumerate connected subgraph complement pairs
  for each v_i ∈ V in reverse order:
    EmitCsg({v_i})
    EnumerateCsgRec({v_i}, {v_0,...,v_{i-1}})    // expand connected subgraphs

  return dpTable[V]

EmitCsg(S₁):
  // For connected subgraph S₁, find all valid complement subgraphs S₂
  N = neighborhood(S₁) \ S₁              // adjacent nodes not in S₁
  for each v ∈ N (in reverse order):
    if dpTable[S₁] exists and dpTable[{v}] exists:
      EmitCsgCmp(S₁, {v})                // evaluate join S₁ ⋈ {v}
    EnumerateCmpRec(S₁, {v}, {v_0,...,v_{j-1}} \ S₁)

EnumerateCsgRec(S, X):
  N = neighborhood(S) ∩ X               // expand S within allowed set X
  for each S' ⊆ N, S' ≠ ∅:
    if dpTable[S ∪ S'] exists:
      EmitCsg(S ∪ S')
    EnumerateCsgRec(S ∪ S', X \ N)
```

The **neighborhood function** returns all nodes adjacent to a set via join edges — this is
what makes DPhyp graph-aware and efficient.

#### IKKBZ — Search Space Linearization

For medium queries (14-100 relations), exact DP becomes too expensive. The IKKBZ algorithm
(Ibaraki & Kameda, extended by Krishnamurthy, Boral & Zaniolo) finds the optimal **left-deep**
tree for acyclic query graphs in polynomial time by sorting joins by rank (cost/benefit ratio).

```
IKKBZ(Q = (V, E)):
  // Construct optimal left-deep tree for acyclic query graph
  b = ∅
  for each v ∈ V:
    P_v = Q directed away from v
    while P_v is not a chain:
      pick v' in P_v that has chains as input
      IKKBZ_normalize each input chain of v'
      merge input chains by rank
    if b = ∅ or C(P_v) < C(b):
      b = P_v
  return b

IKKBZ_normalize(chain c):
  // Sort by T(R) = card * sel ascending (smallest intermediate result first)
  while ∃i: T(c[i]) > T(c[i+1]):
    merge c[i] and c[i+1] into compound relation
```

The linearization converts an arbitrary query graph into a chain, then runs a **simpler O(n³) DP**
on the chain (only considering connected subchains instead of arbitrary subsets).

#### GOO-DP — Greedy Operator Ordering with DP Refinement

For very large queries (100+ relations), even linearized DP becomes expensive. GOO-DP first
builds a greedy bushy plan, then refines subproblems of bounded size with DP:

```
GOO_DP(Q = (V, E), dpAlgorithm, k, budget):
  // Phase 1: Greedy bushy plan (GOO — minimum result cardinality)
  T = V
  while |T| > 1:
    (L, R) = argmin_{L,R ∈ T, L can join R} |L ⋈ R|
    T = T \ {L, R} ∪ {L ⋈ R}
  T = pick single element of T

  // Phase 2: DP refinement on subproblems of size ≤ k
  while budget > 0:
    pick T' ∈ T such that size(T') ≤ k
      and size(T'.parent) > k
      and C(T') is maximal                 // optimize most expensive subtree first
    replace T' with dpAlgorithm(T')
    reduce budget by DP table size from last call
  return T
```

### 10.4 Adaptive Join Order Optimizer Implementation

```java
/**
 * Adaptive join ordering following Neumann & Radke (SIGMOD 2018).
 *
 * Selects the best algorithm based on query graph complexity:
 * - Small queries (< 14 relations or < 10K connected subgraphs): DPhyp (exact)
 * - Medium queries (≤ 100 relations, no hyper-edges): linearized DP (near-optimal)
 * - Large queries: GOO + DP refinement (good heuristic)
 *
 * This is the same approach used by HyPer, Umbra, and DuckDB.
 */
public final class AdaptiveJoinOrderOptimizer {

    private final CostModel costModel;
    private final CardinalityEstimator cardinalityEstimator;

    // Budget threshold for counting connected subgraphs
    private static final int CC_BUDGET = 10_000;
    // Max relations for unconditional DPhyp
    private static final int DPHYP_UNCONDITIONAL_LIMIT = 14;
    // Max relations for linearized DP
    private static final int LINEARIZED_DP_LIMIT = 100;
    // DP refinement subproblem size for GOO-DP
    private static final int GOO_DP_K = 100;

    public AdaptiveJoinOrderOptimizer(CostModel costModel,
                                       CardinalityEstimator cardinalityEstimator) {
        this.costModel = costModel;
        this.cardinalityEstimator = cardinalityEstimator;
    }

    /**
     * Adaptively choose the best join ordering algorithm.
     */
    public AST findOptimalJoinOrder(AST[] relations, JoinGraph joinGraph,
                                     RevisionData revisionData) {
        final int n = relations.length;
        if (n <= 1) return n == 0 ? null : relations[0];

        // Phase 1: Can we use exact DPhyp?
        if (n < DPHYP_UNCONDITIONAL_LIMIT
                || countConnectedSubgraphs(joinGraph, n, CC_BUDGET) <= CC_BUDGET) {
            return dpHyp(relations, joinGraph, revisionData);
        }

        // Phase 2: Medium queries — linearize + chain DP
        if (!joinGraph.hasHyperEdges()) {
            if (n <= LINEARIZED_DP_LIMIT) {
                return linearizedDP(relations, joinGraph, revisionData);
            }
            return gooDp(relations, joinGraph, revisionData, GOO_DP_K);
        }

        // Phase 3: Hyper-edges present — use DPhyp as inner DP with smaller k
        return gooDp(relations, joinGraph, revisionData, 10);
    }

    // ─── DPhyp: Graph-based DP with connected subgraph complement pairs ───

    private AST dpHyp(AST[] relations, JoinGraph joinGraph, RevisionData revisionData) {
        final int n = relations.length;
        // DP table: maps relation BitSet → (cost, cardinality, plan).
        // Pre-size based on estimated connected subgraphs (upper bound 3^n),
        // not just n, to avoid repeated resizing during enumeration.
        int estimatedEntries = Math.min(
            (int) Math.min((long) Math.pow(3, n), MAX_CAPACITY),
            CC_BUDGET * 2);
        final DpTable dpTable = new DpTable(Math.max(n, estimatedEntries));

        // Initialize single-relation base cases
        for (int i = 0; i < n; i++) {
            long card = cardinalityEstimator.estimatePipelineCardinality(
                relations[i], revisionData);
            dpTable.put(1L << i, 0.0, card, relations[i]);
        }

        // Enumerate connected subgraph complement pairs
        // Process nodes in reverse order for correct enumeration
        for (int i = n - 1; i >= 0; i--) {
            long singleNode = 1L << i;
            long excludeSet = (1L << i) - 1; // {v_0, ..., v_{i-1}}
            emitCsg(dpTable, singleNode, joinGraph, n, revisionData);
            enumerateCsgRec(dpTable, singleNode, excludeSet, joinGraph, n, revisionData);
        }

        // Return best plan for full relation set
        long fullSet = (1L << n) - 1;
        return dpTable.getPlan(fullSet);
    }

    private void emitCsg(DpTable dpTable, long s1, JoinGraph joinGraph,
                         int n, RevisionData revisionData) {
        // Find complement subgraphs for s1
        long neighborhood = joinGraph.getNeighborhood(s1, n) & ~s1;

        for (int j = Long.numberOfTrailingZeros(neighborhood); j < n; j++) {
            if ((neighborhood & (1L << j)) == 0) continue;
            long s2 = 1L << j;

            // Try joining s1 ⋈ s2
            emitCsgCmp(dpTable, s1, s2, joinGraph, revisionData);

            // Enumerate larger complements starting from {v_j}
            long excludeSet = ((1L << j) - 1) & ~s1;
            enumerateCmpRec(dpTable, s1, s2, excludeSet, joinGraph, n, revisionData);
        }
    }

    private void emitCsgCmp(DpTable dpTable, long s1, long s2,
                            JoinGraph joinGraph, RevisionData revisionData) {
        if (!dpTable.contains(s1) || !dpTable.contains(s2)) return;

        double cost1 = dpTable.getCost(s1);
        double cost2 = dpTable.getCost(s2);
        long card1 = dpTable.getCardinality(s1);
        long card2 = dpTable.getCardinality(s2);

        // Evaluate join cost
        OperatorContext ctx = OperatorContext.acquire();
        ctx.inputCardinality = card1;
        double joinCost = costModel.estimateCost(XQ.Join, card2, ctx);
        double totalCost = cost1 + cost2 + joinCost;

        long combined = s1 | s2;
        long outputCard = Math.max(1L, (long)(card1 * card2
            * joinGraph.getJoinSelectivity(s1, s2)));

        // Update DP table if this is the best plan for s1 ∪ s2
        if (!dpTable.contains(combined) || totalCost < dpTable.getCost(combined)) {
            AST joinNode = createJoinNode(
                dpTable.getPlan(s1), dpTable.getPlan(s2),
                joinGraph.getJoinPredicate(s1, s2));
            dpTable.put(combined, totalCost, outputCard, joinNode);
        }

        // Also try s2 ⋈ s1 (commutativity — Rule 4).
        // IMPORTANT: Skip reversal for array-index joins (child-index axis is directional,
        // there is no reverse axis). See Section 4, Rule 4.
        if (joinGraph.isArrayIndexEdge(s1, s2)) return; // not commutative

        ctx = OperatorContext.acquire();
        ctx.inputCardinality = card2;
        double reverseCost = cost2 + cost1 + costModel.estimateCost(XQ.Join, card1, ctx);
        if (reverseCost < totalCost
                && (!dpTable.contains(combined) || reverseCost < dpTable.getCost(combined))) {
            AST reverseJoin = createJoinNode(
                dpTable.getPlan(s2), dpTable.getPlan(s1),
                joinGraph.getJoinPredicate(s2, s1));
            dpTable.put(combined, reverseCost, outputCard, reverseJoin);
        }
    }

    private void enumerateCsgRec(DpTable dpTable, long s, long x,
                                  JoinGraph joinGraph, int n, RevisionData revisionData) {
        long neighborhood = joinGraph.getNeighborhood(s, n) & x;
        if (neighborhood == 0) return;

        // Enumerate all non-empty subsets of neighborhood.
        // CRITICAL: recursion must be INSIDE the loop — each expansion S ∪ S'
        // is a new connected subgraph that must be recursively expanded further
        // through its own neighborhood. (Moerkotte & Neumann, SIGMOD 2008)
        for (long subset = neighborhood; subset != 0; subset = (subset - 1) & neighborhood) {
            long expanded = s | subset;
            emitCsg(dpTable, expanded, joinGraph, n, revisionData);
            // Recurse per-subset: expand each connected subgraph further
            enumerateCsgRec(dpTable, expanded, x & ~neighborhood,
                            joinGraph, n, revisionData);
        }
    }

    private void enumerateCmpRec(DpTable dpTable, long s1, long s2, long x,
                                  JoinGraph joinGraph, int n, RevisionData revisionData) {
        long neighborhood = joinGraph.getNeighborhood(s2, n) & x;
        if (neighborhood == 0) return;

        // Enumerate all non-empty subsets of neighborhood.
        // CRITICAL: per-subset recursion — each expanded complement S2 ∪ S' must
        // itself be expanded through its own neighborhood, not just the merged full
        // neighborhood. This matches Moerkotte & Neumann (SIGMOD 2008) Algorithm 3.
        for (long subset = neighborhood; subset != 0; subset = (subset - 1) & neighborhood) {
            long expanded = s2 | subset;
            emitCsgCmp(dpTable, s1, expanded, joinGraph, revisionData);
            enumerateCmpRec(dpTable, s1, expanded, x & ~neighborhood,
                            joinGraph, n, revisionData);
        }
    }

    // ─── Linearized DP: IKKBZ linearization + chain DP ───

    private AST linearizedDP(AST[] relations, JoinGraph joinGraph, RevisionData revisionData) {
        final int n = relations.length;

        // Step 1: IKKBZ linearization — find optimal relation ordering
        int[] linearOrder = ikkbzLinearize(relations, joinGraph, revisionData);

        // Step 2: Chain DP on the linearized order — O(n³)
        // Only considers connected subchains [i..j], not arbitrary subsets
        final double[][] chainCost = new double[n][n];     // chainCost[i][j] = best cost for [i..j]
        final long[][] chainCard = new long[n][n];
        final AST[][] chainPlan = new AST[n][n];

        // Base cases: single relations
        for (int i = 0; i < n; i++) {
            int relIdx = linearOrder[i];
            chainCost[i][i] = 0.0;
            chainCard[i][i] = cardinalityEstimator.estimatePipelineCardinality(
                relations[relIdx], revisionData);
            chainPlan[i][i] = relations[relIdx];
        }

        // Fill DP table for increasing chain lengths
        for (int len = 2; len <= n; len++) {
            for (int i = 0; i <= n - len; i++) {
                int j = i + len - 1;
                double bestCost = Double.MAX_VALUE;

                // Try all split points k in [i, j-1]
                for (int k = i; k < j; k++) {
                    OperatorContext ctx = OperatorContext.acquire();
                    ctx.inputCardinality = chainCard[i][k];
                    double joinCost = costModel.estimateCost(XQ.Join, chainCard[k + 1][j], ctx);
                    double total = chainCost[i][k] + chainCost[k + 1][j] + joinCost;

                    if (total < bestCost) {
                        bestCost = total;
                        chainCost[i][j] = total;
                        // Use actual join selectivity from join graph, not hardcoded 0.1
                        long leftSet = relationBitmask(linearOrder, i, k);
                        long rightSet = relationBitmask(linearOrder, k + 1, j);
                        double joinSel = joinGraph.getJoinSelectivity(leftSet, rightSet);
                        chainCard[i][j] = Math.max(1L,
                            (long)(chainCard[i][k] * chainCard[k + 1][j] * joinSel));
                        chainPlan[i][j] = createJoinNode(
                            chainPlan[i][k], chainPlan[k + 1][j], null);
                    }
                }
            }
        }

        return chainPlan[0][n - 1];
    }

    /**
     * IKKBZ linearization: sort relations by join selectivity rank.
     * For acyclic query graphs, this produces the optimal left-deep ordering.
     * For cyclic graphs, we first compute a minimum spanning tree.
     */
    private int[] ikkbzLinearize(AST[] relations, JoinGraph joinGraph, RevisionData revisionData) {
        final int n = relations.length;
        int[] order = new int[n];

        // IKKBZ rank: the KBZ/IKKBZ algorithm (Krishnamurthy, Boral, Zaniolo 1986)
        // sorts relations by the benefit ratio (T(R) - 1) / C(R), but for the
        // simplified formulation, we rank by T(R) = card * sel (estimated
        // intermediate result size). Joining smaller intermediates first minimizes
        // the Cartesian product growth. Lower T = higher priority = join earlier.
        double[] ranks = new double[n];
        for (int i = 0; i < n; i++) {
            long card = cardinalityEstimator.estimatePipelineCardinality(
                relations[i], revisionData);
            double sel = joinGraph.getMinSelectivity(i);
            ranks[i] = Math.max(1.0, card * sel); // T(R): intermediate result size
            order[i] = i;
        }

        // Sort by T(R) ascending: smallest intermediate result first
        // Simple insertion sort — n is bounded by LINEARIZED_DP_LIMIT (100)
        for (int i = 1; i < n; i++) {
            int key = order[i];
            double keyRank = ranks[key];
            int j = i - 1;
            while (j >= 0 && ranks[order[j]] > keyRank) {
                order[j + 1] = order[j];
                j--;
            }
            order[j + 1] = key;
        }

        return order;
    }

    /** Build bitmask for linearized relation range [from..to] */
    private long relationBitmask(int[] linearOrder, int from, int to) {
        long mask = 0L;
        for (int i = from; i <= to; i++) {
            mask |= (1L << linearOrder[i]);
        }
        return mask;
    }

    // ─── GOO-DP: Greedy + DP refinement for very large queries ───

    private AST gooDp(AST[] relations, JoinGraph joinGraph,
                      RevisionData revisionData, int maxSubproblemSize) {
        final int n = relations.length;

        // Phase 1: Greedy Operator Ordering — build initial bushy plan
        // Repeatedly join the two relations with minimum result cardinality
        AST[] pool = new AST[n];
        long[] cards = new long[n];
        long[] sets = new long[n];
        int poolSize = n;

        for (int i = 0; i < n; i++) {
            pool[i] = relations[i];
            cards[i] = cardinalityEstimator.estimatePipelineCardinality(
                relations[i], revisionData);
            sets[i] = 1L << i;
        }

        while (poolSize > 1) {
            double bestJoinCard = Double.MAX_VALUE;
            int bestL = -1;
            int bestR = -1;

            // Find pair with minimum join result cardinality
            for (int l = 0; l < poolSize; l++) {
                for (int r = l + 1; r < poolSize; r++) {
                    if (!joinGraph.hasJoinPredicate(sets[l], sets[r])) continue;
                    long joinCard = Math.max(1L, (long)(cards[l] * cards[r]
                        * joinGraph.getJoinSelectivity(sets[l], sets[r])));
                    if (joinCard < bestJoinCard) {
                        bestJoinCard = joinCard;
                        bestL = l;
                        bestR = r;
                    }
                }
            }

            if (bestL < 0) {
                // Disconnected graph — force Cartesian product of remaining components.
                // Find the two smallest remaining relations to minimize cross-product cost.
                if (poolSize >= 2) {
                    bestL = 0; bestR = 1;
                    if (cards[1] < cards[0]) { bestL = 1; bestR = 0; }
                    for (int k = 2; k < poolSize; k++) {
                        if (cards[k] < cards[bestL]) {
                            bestR = bestL; bestL = k;
                        } else if (cards[k] < cards[bestR]) {
                            bestR = k;
                        }
                    }
                    bestJoinCard = cards[bestL] * cards[bestR]; // Cartesian product
                } else {
                    break;
                }
            }

            // Merge bestL and bestR
            AST joined = createJoinNode(pool[bestL], pool[bestR],
                joinGraph.getJoinPredicate(sets[bestL], sets[bestR]));
            pool[bestL] = joined;
            cards[bestL] = (long) bestJoinCard;
            sets[bestL] = sets[bestL] | sets[bestR];

            // Remove bestR from pool (swap with last)
            pool[bestR] = pool[poolSize - 1];
            cards[bestR] = cards[poolSize - 1];
            sets[bestR] = sets[poolSize - 1];
            poolSize--;
        }

        // Phase 2: DP refinement on expensive subproblems of size ≤ k
        // (Implementation iterates over join tree, finding subtrees to re-optimize)
        // Budget = CC_BUDGET total DP table entries across all refinements
        // Each refinement replaces a subtree with the DP-optimal plan
        AST greedyPlan = pool[0];
        return refinePlanWithDP(greedyPlan, joinGraph, revisionData,
                                maxSubproblemSize, CC_BUDGET);
    }

    private AST refinePlanWithDP(AST plan, JoinGraph joinGraph,
                                  RevisionData revisionData,
                                  int maxSubproblemSize, int budget) {
        // Walk the join tree bottom-up, collecting subtrees of size ≤ k
        // that have a parent subtree of size > k (boundary subtrees).
        // Re-optimize the most expensive boundary subtree first.
        final ObjectArrayList<JoinSubtree> candidates = new ObjectArrayList<>();
        collectRefinementCandidates(plan, joinGraph, maxSubproblemSize, candidates);

        // Sort by descending cost: optimize most expensive subtree first
        candidates.sort((a, b) -> Double.compare(b.estimatedCost, a.estimatedCost));

        int remainingBudget = budget;
        for (JoinSubtree candidate : candidates) {
            if (remainingBudget <= 0) break;

            // Extract relations from this subtree
            AST[] subRelations = candidate.extractRelations();
            if (subRelations.length <= 1) continue;

            // Re-optimize with exact DPhyp
            AST optimized = dpHyp(subRelations, joinGraph, revisionData);
            if (optimized != null) {
                // Replace subtree in parent plan
                candidate.replaceInParent(optimized);
                // Deduct budget by estimated DP table entries for this subproblem
                // Upper bound: 3^n connected subgraphs for n relations
                remainingBudget -= Math.min(remainingBudget,
                    (int) Math.pow(3, subRelations.length));
            }
        }
        return plan;
    }

    private void collectRefinementCandidates(AST node, JoinGraph joinGraph,
                                              int maxSize,
                                              ObjectArrayList<JoinSubtree> candidates) {
        if (node == null || node.getType() != XQ.Join) return;

        int subtreeSize = countJoinRelations(node);
        if (subtreeSize <= maxSize) {
            // This subtree is small enough to re-optimize with DP
            candidates.add(new JoinSubtree(node, subtreeSize, estimateSubtreeCost(node)));
        } else {
            // Recurse into children to find smaller boundary subtrees
            for (int i = 0; i < node.getChildCount(); i++) {
                collectRefinementCandidates(node.getChild(i), joinGraph, maxSize, candidates);
            }
        }
    }

    private int countJoinRelations(AST node) {
        if (node == null) return 0;
        if (node.getType() != XQ.Join) return 1; // leaf relation
        int count = 0;
        for (int i = 0; i < node.getChildCount(); i++) {
            count += countJoinRelations(node.getChild(i));
        }
        return count;
    }

    private double estimateSubtreeCost(AST node) {
        // Simple heuristic: deeper trees are more expensive
        if (node == null || node.getType() != XQ.Join) return 0.0;
        double childCost = 0.0;
        for (int i = 0; i < node.getChildCount(); i++) {
            childCost += estimateSubtreeCost(node.getChild(i));
        }
        return 1.0 + childCost;
    }

    record JoinSubtree(AST root, int size, double estimatedCost) {
        AST[] extractRelations() {
            ObjectArrayList<AST> relations = new ObjectArrayList<>();
            collectLeafRelations(root, relations);
            return relations.toArray(new AST[0]);
        }
        private void collectLeafRelations(AST node, ObjectArrayList<AST> out) {
            if (node == null) return;
            if (node.getType() != XQ.Join) { out.add(node); return; }
            for (int i = 0; i < node.getChildCount(); i++) {
                collectLeafRelations(node.getChild(i), out);
            }
        }
        void replaceInParent(AST replacement) {
            AST parent = root.getParent();
            if (parent != null) {
                parent.replaceChild(root.getChildIndex(), replacement);
            }
        }
    }

    // ─── Utility: count connected subgraphs ───

    /**
     * Count connected subgraphs up to a budget (Neumann & Radke Figure 3).
     * Returns early if budget is exceeded — O(budget) time.
     */
    private int countConnectedSubgraphs(JoinGraph joinGraph, int n, int budget) {
        int count = 0;
        for (int i = 0; i < n; i++) {
            if (count > budget) return count;
            long startNode = 1L << i;
            long excludeSet = (1L << i) - 1; // nodes before i
            count = countCCRec(joinGraph, startNode, excludeSet, n, count, budget);
        }
        return count;
    }

    private int countCCRec(JoinGraph joinGraph, long s, long x,
                           int n, int count, int budget) {
        long neighborhood = joinGraph.getNeighborhood(s, n) & x;
        for (long subset = neighborhood; subset != 0; subset = (subset - 1) & neighborhood) {
            count++;
            if (count > budget) return count;
            count = countCCRec(joinGraph, s | subset, x & ~neighborhood, n, count, budget);
            if (count > budget) return count;
        }
        return count;
    }

    // ─── AST construction ───

    private AST createJoinNode(AST left, AST right, AST joinPredicate) {
        AST join = new AST(XQ.Join);
        AST leftStart = new AST(XQ.Start);
        leftStart.addChild(left);
        join.addChild(leftStart);
        AST rightStart = new AST(XQ.Start);
        rightStart.addChild(right);
        join.addChild(rightStart);
        AST postStart = new AST(XQ.Start);
        postStart.addChild(new AST(XQ.End));
        join.addChild(postStart);
        if (joinPredicate != null) {
            join.setProperty("cmp", joinPredicate.getProperty("cmp"));
        }
        return join;
    }
}

/**
 * DP table using long bitmasks as keys for relation sets.
 * For ≤ 63 relations, uses a single long per set (bit 63 reserved as sentinel).
 * Uses open-addressing hash map with Robin Hood hashing for cache efficiency.
 *
 * IMPORTANT: Sentinel is Long.MIN_VALUE (0x8000000000000000), NOT -1L.
 * -1L (all bits set) is a valid key for 64 relations.
 * We limit to 63 relations (bits 0-62) so bit 63 is always clear in valid keys.
 *
 * Sizing is based on countCC (connected subgraph count), NOT 2^n,
 * to avoid OOM for large queries. Max capacity is bounded.
 */
final class DpTable {
    private static final long EMPTY = Long.MIN_VALUE; // sentinel: bit 63 set
    private static final int MAX_CAPACITY = 1 << 24;  // 16M entries (~512 MB max)

    private long[] keys;
    private double[] costs;
    private long[] cardinalities;
    private AST[] plans;
    private int capacity;
    private int mask;
    private int size;

    DpTable(int estimatedEntries) {
        // Size to next power of 2 with 50% max load factor.
        // Use long arithmetic to prevent overflow for large estimatedEntries.
        long rawSize = Math.max(16L, (long) Integer.highestOneBit(
            Math.max(1, estimatedEntries - 1)) << 2);
        this.capacity = (int) Math.min(rawSize, MAX_CAPACITY);
        this.mask = capacity - 1;
        this.keys = new long[capacity];
        this.costs = new double[capacity];
        this.cardinalities = new long[capacity];
        this.plans = new AST[capacity];
        this.size = 0;
        java.util.Arrays.fill(keys, EMPTY);
    }

    void put(long set, double cost, long cardinality, AST plan) {
        assert set != EMPTY : "relation set collides with sentinel";

        // Resize at 75% load to prevent infinite probe chains.
        // If at MAX_CAPACITY and resize is a no-op, reject the insert to
        // prevent load factor from approaching 100% (which causes O(n)
        // probe chains and eventual infinite loop in find()).
        if (size >= (capacity * 3L) / 4) {
            int oldCapacity = capacity;
            resize();
            if (capacity == oldCapacity && size >= (capacity * 3L) / 4) {
                // Cannot grow — check if this is an update (existing key)
                int existing = find(set);
                if (existing < 0) {
                    throw new IllegalStateException(
                        "DpTable exceeded max capacity " + MAX_CAPACITY
                        + " — query too complex for exact optimization");
                }
                // Update in place (existing key, no new slot needed)
                costs[existing] = cost;
                cardinalities[existing] = cardinality;
                plans[existing] = plan;
                return;
            }
        }

        int idx = fibHash(set);
        while (keys[idx] != EMPTY && keys[idx] != set) {
            idx = (idx + 1) & mask;
        }
        if (keys[idx] == EMPTY) size++;
        keys[idx] = set;
        costs[idx] = cost;
        cardinalities[idx] = cardinality;
        plans[idx] = plan;
    }

    boolean contains(long set) {
        return find(set) >= 0;
    }

    double getCost(long set) {
        int idx = find(set);
        return idx >= 0 ? costs[idx] : Double.MAX_VALUE;
    }

    long getCardinality(long set) {
        int idx = find(set);
        return idx >= 0 ? cardinalities[idx] : 0L;
    }

    AST getPlan(long set) {
        int idx = find(set);
        return idx >= 0 ? plans[idx] : null;
    }

    /** Fibonacci hashing — better distribution than Long.hashCode for bitmask keys. */
    private int fibHash(long set) {
        return (int) ((set * 0x9E3779B97F4A7C15L) >>> (64 - Integer.numberOfTrailingZeros(capacity)));
    }

    private int find(long set) {
        int idx = fibHash(set);
        while (keys[idx] != EMPTY) {
            if (keys[idx] == set) return idx;
            idx = (idx + 1) & mask;
        }
        return -1;
    }

    private void resize() {
        if (capacity >= MAX_CAPACITY) return; // hard limit reached
        int newCapacity = capacity << 1;
        long[] oldKeys = keys;
        double[] oldCosts = costs;
        long[] oldCards = cardinalities;
        AST[] oldPlans = plans;

        capacity = newCapacity;
        mask = newCapacity - 1;
        keys = new long[newCapacity];
        costs = new double[newCapacity];
        cardinalities = new long[newCapacity];
        plans = new AST[newCapacity];
        java.util.Arrays.fill(keys, EMPTY);
        size = 0;

        for (int i = 0; i < oldKeys.length; i++) {
            if (oldKeys[i] != EMPTY) {
                put(oldKeys[i], oldCosts[i], oldCards[i], oldPlans[i]);
            }
        }
    }
}
```

### 10.5 Join Graph with Neighborhood Function

DPhyp requires an efficient **neighborhood function** that returns all nodes adjacent to a
relation set. We also track join selectivities and hyper-edges (for non-inner joins):

```java
public final class JoinGraph {
    // Per-relation adjacency: bitmask for fast neighborhood lookups in DPhyp.
    private final long[] neighborBitmasks;

    // For small graphs (≤ 64 relations): dense n×n matrix — cache-friendly, O(1) lookup.
    // For large graphs (> 64 relations): use sparse edge list instead.
    // The threshold matches our bitmask capacity (63 relations max for DPhyp).
    private static final int SPARSE_THRESHOLD = 64;

    // Dense storage (used when size ≤ SPARSE_THRESHOLD)
    private final AST[][] joinPredicates;
    private final double[][] joinSelectivities;

    // Sparse storage (used when size > SPARSE_THRESHOLD, e.g. GOO-DP with 5000+ relations)
    // Key = (rel1 << 16 | rel2), stored as long for zero-boxing lookup
    private final Long2ObjectOpenHashMap<JoinEdge> sparseEdges;

    private final int size;
    private final boolean useSparse;
    private boolean hasHyperEdges;

    public JoinGraph(int size) {
        this.size = size;
        this.useSparse = size > SPARSE_THRESHOLD;
        this.neighborBitmasks = new long[size];
        this.hasHyperEdges = false;

        if (useSparse) {
            // Sparse: O(edges) memory instead of O(n²)
            this.joinPredicates = null;
            this.joinSelectivities = null;
            this.sparseEdges = new Long2ObjectOpenHashMap<>();
        } else {
            // Dense: fast matrix lookup for small graphs
            this.joinPredicates = new AST[size][size];
            this.joinSelectivities = new double[size][size];
            this.sparseEdges = null;
            for (double[] row : joinSelectivities) {
                java.util.Arrays.fill(row, 0.1);
            }
        }
    }

    /** Encode two relation IDs into a single long key. Uses full 32 bits per ID
     *  to support up to Integer.MAX_VALUE relations without collision. */
    private static long edgeKey(int rel1, int rel2) {
        return ((long) Math.min(rel1, rel2) << 32) | (Math.max(rel1, rel2) & 0xFFFFFFFFL);
    }

    public void addJoinPredicate(int rel1, int rel2, AST predicate, double selectivity) {
        // Bitmask-based neighborhood only works for ≤ 63 relations (bits 0-62).
        // For larger graphs (GOO-DP path), neighborBitmasks are not used —
        // GOO-DP uses hasJoinPredicate() which falls through to sparse edge lookup.
        if (rel1 < 63 && rel2 < 63) {
            neighborBitmasks[rel1] |= (1L << rel2);
            neighborBitmasks[rel2] |= (1L << rel1);
        }
        if (useSparse) {
            sparseEdges.put(edgeKey(rel1, rel2),
                new JoinEdge(rel1, rel2, predicate, selectivity));
        } else {
            joinPredicates[rel1][rel2] = predicate;
            joinPredicates[rel2][rel1] = predicate;
            joinSelectivities[rel1][rel2] = selectivity;
            joinSelectivities[rel2][rel1] = selectivity;
        }
    }

    /**
     * The neighborhood function — core of DPhyp.
     * Returns bitmask of all nodes adjacent to any node in the given set.
     * O(popcount(set)) — very fast due to bitmask OR operations.
     *
     * IMPORTANT: Only valid for sets within the first 63 relations (DPhyp path).
     * GOO-DP for >63 relations must use hasJoinPredicate() instead.
     */
    public long getNeighborhood(long set, int n) {
        long result = 0L;
        for (long remaining = set; remaining != 0; ) {
            int i = Long.numberOfTrailingZeros(remaining);
            if (i < neighborBitmasks.length) {
                result |= neighborBitmasks[i];
            }
            remaining &= remaining - 1; // clear lowest bit
        }
        return result;
    }

    public boolean hasJoinPredicate(long subset1, long subset2) {
        // For sparse mode, check edge map directly (handles >63 relations)
        if (useSparse) {
            for (long r1 = subset1; r1 != 0; ) {
                int i = Long.numberOfTrailingZeros(r1);
                for (long r2 = subset2; r2 != 0; ) {
                    int j = Long.numberOfTrailingZeros(r2);
                    if (sparseEdges.containsKey(edgeKey(i, j))) return true;
                    r2 &= r2 - 1;
                }
                r1 &= r1 - 1;
            }
            return false;
        }
        // Dense mode: use bitmask neighborhood
        for (long remaining = subset1; remaining != 0; ) {
            int i = Long.numberOfTrailingZeros(remaining);
            if ((neighborBitmasks[i] & subset2) != 0) return true;
            remaining &= remaining - 1;
        }
        return false;
    }

    public AST getJoinPredicate(long subset1, long subset2) {
        for (long r1 = subset1; r1 != 0; ) {
            int i = Long.numberOfTrailingZeros(r1);
            for (long r2 = subset2 & neighborBitmasks[i]; r2 != 0; ) {
                int j = Long.numberOfTrailingZeros(r2);
                if (useSparse) {
                    JoinEdge edge = sparseEdges.get(edgeKey(i, j));
                    if (edge != null) return edge.predicate();
                } else {
                    if (joinPredicates[i][j] != null) return joinPredicates[i][j];
                }
                r2 &= r2 - 1;
            }
            r1 &= r1 - 1;
        }
        return null;
    }

    public double getJoinSelectivity(long subset1, long subset2) {
        // Use minimum selectivity among all join edges between the subsets
        double minSel = 1.0;
        for (long r1 = subset1; r1 != 0; ) {
            int i = Long.numberOfTrailingZeros(r1);
            for (long r2 = subset2 & neighborBitmasks[i]; r2 != 0; ) {
                int j = Long.numberOfTrailingZeros(r2);
                if (useSparse) {
                    JoinEdge edge = sparseEdges.get(edgeKey(i, j));
                    if (edge != null) minSel = Math.min(minSel, edge.selectivity());
                } else {
                    minSel = Math.min(minSel, joinSelectivities[i][j]);
                }
                r2 &= r2 - 1;
            }
            r1 &= r1 - 1;
        }
        return minSel;
    }

    public double getMinSelectivity(int rel) {
        double min = 1.0;
        for (int j = 0; j < size; j++) {
            if ((neighborBitmasks[rel] & (1L << j)) != 0) {
                if (useSparse) {
                    JoinEdge edge = sparseEdges.get(edgeKey(rel, j));
                    if (edge != null) min = Math.min(min, edge.selectivity());
                } else {
                    min = Math.min(min, joinSelectivities[rel][j]);
                }
            }
        }
        return min;
    }

    /**
     * Check if the join edge between two sets involves an array-index axis.
     * Array-index joins are NOT commutative (Rule 4 exclusion).
     */
    public boolean isArrayIndexEdge(long subset1, long subset2) {
        for (long r1 = subset1; r1 != 0; ) {
            int i = Long.numberOfTrailingZeros(r1);
            for (long r2 = subset2 & neighborBitmasks[i]; r2 != 0; ) {
                int j = Long.numberOfTrailingZeros(r2);
                AST pred;
                if (useSparse) {
                    JoinEdge edge = sparseEdges.get(edgeKey(i, j));
                    pred = edge != null ? edge.predicate() : null;
                } else {
                    pred = joinPredicates[i][j];
                }
                if (pred != null && "child-index".equals(pred.getProperty("axis"))) {
                    return true;
                }
                r2 &= r2 - 1;
            }
            r1 &= r1 - 1;
        }
        return false;
    }

    public boolean hasHyperEdges() {
        return hasHyperEdges;
    }

    public void markHyperEdge() {
        this.hasHyperEdges = true;
    }
}

record JoinEdge(int rel1, int rel2, AST predicate, double selectivity) {}
```

---

## 11. Optimizer Stage Integration

### 11.1 JQGM Rewrite Stage (Rules 1-4)

```java
/**
 * Applies Weiner's rewrite rules 1-4 adapted for JSON:
 * - Rule 1: Fusion of adjacent JSON path joins
 * - Rule 2: Fusion of Select + JSON path join
 * - Rule 3: Fusion of Select + ObjectAccess/ArrayAccess
 * - Rule 4: Commutativity of binary JSON path joins
 *
 * Location: sirix-query/.../compiler/optimizer/JqgmRewriteStage.java
 */
public final class JqgmRewriteStage implements Stage {

    @Override
    public AST rewrite(StaticContext sctx, AST ast) {
        // Apply rules in order, iterating until fixpoint.
        // Walkers return the SAME AST reference if no modification was made
        // (Brackit Walker contract), so identity comparison (==) detects fixpoint.
        // Fallback: bounded iteration prevents infinite loops if a rule
        // oscillates between equivalent forms.
        boolean modified = true;
        int iterations = 0;
        while (modified && iterations < 10) {
            modified = false;
            AST prev = ast;
            ast = new JoinFusionWalker().walk(ast);          // Rule 1
            if (ast != prev) { modified = true; prev = ast; }
            ast = new SelectJoinFusionWalker().walk(ast);    // Rule 2
            if (ast != prev) { modified = true; prev = ast; }
            ast = new SelectAccessFusionWalker().walk(ast);  // Rule 3
            if (ast != prev) { modified = true; prev = ast; }
            ast = new JoinCommutativityWalker().walk(ast);   // Rule 4
            if (ast != prev) { modified = true; }
            iterations++;
        }
        return ast;
    }
}
```

### 11.2 Cost-Based Stage

```java
/**
 * Two-phase optimization (Weiner Section 4.3):
 * Phase 1: Heuristic initial QEP (already done by prior Brackit stages)
 * Phase 2: Cost-based refinement using statistics and the Mesh
 *
 * Location: sirix-query/.../compiler/optimizer/CostBasedStage.java
 */
public final class CostBasedStage implements Stage {

    private final JsonDBStore jsonStore;
    private final CostModel costModel;
    private SirixStatisticsProvider statsProvider;
    private SelectivityEstimator selectivityEstimator;
    private CardinalityEstimator cardinalityEstimator;
    private AdaptiveJoinOrderOptimizer joinOrderOptimizer;

    public CostBasedStage(JsonDBStore jsonStore) {
        this.jsonStore = jsonStore;
        this.costModel = new JsonCostModel();
    }

    /** Accessor for sharing the statistics provider with IndexDecompositionStage. */
    public StatisticsProvider getStatsProvider() {
        if (statsProvider == null) {
            statsProvider = new SirixStatisticsProvider(jsonStore);
            selectivityEstimator = new SelectivityEstimator(statsProvider);
            cardinalityEstimator = new CardinalityEstimator(statsProvider, selectivityEstimator);
            joinOrderOptimizer = new AdaptiveJoinOrderOptimizer(costModel, cardinalityEstimator);
        }
        return statsProvider;
    }

    @Override
    public AST rewrite(StaticContext sctx, AST ast) {
        // Lazy-init the estimator pipeline once (or reuse if getStatsProvider()
        // was called first by SirixOptimizer), then clear per-query caches.
        if (statsProvider == null) {
            getStatsProvider(); // initializes all fields
        } else {
            // Clear per-query caches to avoid stale statistics from prior queries
            statsProvider.clearCaches();
        }

        // Phase 2a: Cost-based join reordering
        ast = new CostBasedJoinReorder(jsonStore, joinOrderOptimizer, cardinalityEstimator).walk(ast);

        // Phase 2b: Cost-based index selection
        ast = new CostBasedIndexSelection(jsonStore, statsProvider, costModel).walk(ast);

        return ast;
    }
}
```

### 11.3 Index Decomposition Stage (Rules 5-6)

```java
/**
 * Applies Weiner's rewrite rules 5-6 for index-aware join decomposition:
 * - Rule 5: Split n-way join when one path covered by index
 * - Rule 6: Split when multiple indexes overlap at JPP root
 *
 * Location: sirix-query/.../compiler/optimizer/IndexDecompositionStage.java
 */
public final class IndexDecompositionStage implements Stage {

    private final StatisticsProvider statsProvider;

    /**
     * Accepts a shared StatisticsProvider to avoid opening duplicate resource
     * sessions. The same provider instance should be shared with CostBasedStage.
     */
    public IndexDecompositionStage(StatisticsProvider statsProvider) {
        this.statsProvider = statsProvider;
    }

    @Override
    public AST rewrite(StaticContext sctx, AST ast) {
        ast = new JoinDecompositionWalker(statsProvider).walk(ast);  // Rules 5 & 6
        return ast;
    }
}
```

### 11.4 Modified SirixOptimizer

```java
public final class SirixOptimizer extends TopDownOptimizer {

    public SirixOptimizer(final Map<QNm, Str> options,
                          final XmlDBStore nodeStore,
                          final JsonDBStore jsonItemStore) {
        super(options);

        // Shared statistics provider — avoids duplicate resource sessions
        // across CostBasedStage and IndexDecompositionStage
        final var costBasedStage = new CostBasedStage(jsonItemStore);

        // NEW: JQGM rewrite rules (Rules 1-4) — inserted before cost-based stage
        addStageBeforeIndexMatching(new JqgmRewriteStage());

        // NEW: Cost-based optimization (join ordering + index selection)
        addStageBeforeIndexMatching(costBasedStage);

        // NEW: Index-aware join decomposition (Rules 5-6)
        // Shares the same statistics provider instance via CostBasedStage
        addStageBeforeIndexMatching(
            new IndexDecompositionStage(costBasedStage.getStatsProvider()));

        // Existing: Perform index matching as last step
        getStages().add(new IndexMatching(nodeStore, jsonItemStore));
    }
}
```

---

## 12. Cost-Based Walker Implementations

### 12.1 Cost-Based Join Reorder Walker

```java
public final class CostBasedJoinReorder extends ScopeWalker {

    private final JsonDBStore jsonStore;
    private final AdaptiveJoinOrderOptimizer joinOrderOptimizer;
    private final CardinalityEstimator cardinalityEstimator;

    @Override
    protected AST visit(AST node) {
        if (node.getType() != XQ.Join) return node;

        // Collect all join nodes in this join group
        List<AST> joinNodes = new ArrayList<>();
        List<AST> relations = new ArrayList<>();
        JoinGraph joinGraph = collectJoinGroup(node, joinNodes, relations);

        if (relations.size() <= 2) return node; // no reordering needed

        RevisionData revisionData = extractRevisionData(relations.get(0));
        if (revisionData == null) return node;

        AST[] relArray = relations.toArray(new AST[0]);
        AST optimalPlan = joinOrderOptimizer.findOptimalJoinOrder(
            relArray, joinGraph, revisionData);

        if (optimalPlan != null && optimalPlan != node) {
            AST parent = node.getParent();
            parent.replaceChild(node.getChildIndex(), optimalPlan);
            return optimalPlan;
        }
        return node;
    }

    private JoinGraph collectJoinGroup(AST join, List<AST> joins, List<AST> relations) {
        // Walk join tree collecting all relations and predicates
        // Build JoinGraph from collected information
        return new JoinGraph(relations.size());
    }

    private RevisionData extractRevisionData(AST node) {
        // Extract database/resource/revision from jn:doc() call
        return null;
    }
}
```

### 12.2 Cost-Based Index Selection Walker

```java
public final class CostBasedIndexSelection extends ScopeWalker {

    private final JsonDBStore jsonStore;
    private final StatisticsProvider statsProvider;
    private final CostModel costModel;

    @Override
    protected AST visit(AST node) {
        if (node.getType() != XQ.Selection) return node;

        AST predicate = node.getChild(0);
        RevisionData revisionData = extractRevisionData(node);
        if (revisionData == null) return node;

        Path<QNm> path = extractPathFromPredicate(predicate);
        if (path == null) return node;

        IndexInfo indexInfo = statsProvider.getIndexInfo(
            path, revisionData.databaseName(),
            revisionData.resourceName(), revisionData.revision());

        if (!indexInfo.exists()) return node;

        // Cost comparison: sequential scan vs index scan
        // (Weiner Section 4.3.1: "an element index scan—if present—is
        //  the access method of choice")
        long pathCard = statsProvider.getPathCardinality(
            path, revisionData.databaseName(),
            revisionData.resourceName(), revisionData.revision());
        long totalNodes = statsProvider.getTotalNodeCount(
            revisionData.databaseName(), revisionData.resourceName(),
            revisionData.revision());

        OperatorContext seqCtx = OperatorContext.acquire();
        seqCtx.isSequentialScan = true;
        seqCtx.inputCardinality = totalNodes;
        double seqScanCost = costModel.estimateCost(XQ.ForBind, totalNodes, seqCtx);

        OperatorContext idxCtx = OperatorContext.acquire();
        idxCtx.isIndexScan = true;
        idxCtx.indexId = indexInfo.indexId();
        idxCtx.inputCardinality = pathCard;
        double indexScanCost = costModel.estimateCost(XQ.ForBind, pathCard, idxCtx);

        if (costModel.compareCosts(indexScanCost, seqScanCost) < 0) {
            node.setProperty("preferIndex", true);
            node.setProperty("indexId", indexInfo.indexId());
            node.setProperty("indexType", indexInfo.type());
        }

        return node;
    }
}
```

---

## 13. Implementation Phases

### Phase 1: Foundation (Weeks 1-2)

**Goal**: Core infrastructure — cost model, statistics provider, profiles

**Files to Create**:
| Location | File | Description |
|---|---|---|
| brackit `optimizer/cost/` | `CostModel.java` | Enhanced cost model interface with `estimatePhysicalOperatorCost` |
| brackit `optimizer/cost/` | `OperatorContext.java` | Extended with Weiner profile fields |
| brackit `optimizer/cost/` | `JsonCostModel.java` | JSON-aware cost model with I/O + CPU cost |
| brackit `optimizer/cost/` | `PhysicalOperator.java` | Enum of physical operators |
| sirix-query `optimizer/stats/` | `StatisticsProvider.java` | Interface with `buildBaseProfile`, `getAncestorPathCardinality` |
| sirix-query `optimizer/stats/` | `SirixStatisticsProvider.java` | Implementation using PathSummary |
| sirix-query `optimizer/stats/` | `BaseProfile.java` | Record with physical storage stats |
| sirix-query `optimizer/stats/` | `IntermediateProfile.java` | Record with derived stats |
| sirix-query `optimizer/stats/` | `IndexInfo.java` | Record for index metadata |
| sirix-query `optimizer/stats/` | `RevisionData.java` | Record for db/resource/revision context |

### Phase 2: Estimation (Weeks 3-4)

**Goal**: Selectivity and cardinality estimation, profile propagation

**Files to Create**:
| Location | File | Description |
|---|---|---|
| sirix-query `optimizer/stats/` | `SelectivityEstimator.java` | Predicate selectivity with AXS-Stat-style defaults |
| sirix-query `optimizer/stats/` | `CardinalityEstimator.java` | Pipeline cardinality with profile hierarchy propagation |
| sirix-query `optimizer/stats/` | `JsonStatCollector.java` | JSON-Stat collection (adapted from AXS-Stat) |
| test | `SelectivityEstimatorTest.java` | Unit tests |
| test | `CardinalityEstimatorTest.java` | Unit tests |

### Phase 3: JQGM Rewrite Rules (Weeks 5-6)

**Goal**: Implement Weiner's 6 rewrite rules adapted for JSON

**Files to Create**:
| Location | File | Description |
|---|---|---|
| sirix-query `optimizer/` | `JqgmRewriteStage.java` | Stage orchestrating Rules 1-4 |
| sirix-query `optimizer/walker/` | `JoinFusionWalker.java` | Rule 1: fuse adjacent JSON path joins |
| sirix-query `optimizer/walker/` | `SelectJoinFusionWalker.java` | Rule 2: fuse Select + Join |
| sirix-query `optimizer/walker/` | `SelectAccessFusionWalker.java` | Rule 3: fuse Select + ObjectAccess |
| sirix-query `optimizer/walker/` | `JoinCommutativityWalker.java` | Rule 4: commutativity |
| sirix-query `optimizer/` | `IndexDecompositionStage.java` | Stage for Rules 5-6 |
| sirix-query `optimizer/walker/` | `JoinDecompositionWalker.java` | Rules 5-6: index-aware decomposition |
| test | `JqgmRewriteRulesTest.java` | Tests for all 6 rules |

### Phase 4: Join Ordering & Cost-Based Stage (Weeks 7-8)

**Goal**: Adaptive DPhyp join ordering (Neumann & Radke), Mesh data structure, cost-based stage

**Files to Create**:
| Location | File | Description |
|---|---|---|
| sirix-query `optimizer/cost/` | `AdaptiveJoinOrderOptimizer.java` | DPhyp + linearizedDP + GOO-DP adaptive strategy |
| sirix-query `optimizer/cost/` | `DpTable.java` | Open-addressing hash map for DP memoization |
| sirix-query `optimizer/cost/` | `JoinGraph.java` | Neighborhood function, bitmask adjacency, hyper-edge support |
| sirix-query `optimizer/cost/` | `Mesh.java` | Search space management (Graefe/DeWitt) |
| sirix-query `optimizer/cost/` | `EquivalenceClass.java` | Equivalence class with plan alternatives |
| sirix-query `optimizer/walker/` | `CostBasedJoinReorder.java` | Walker for join reordering |
| sirix-query `optimizer/walker/` | `CostBasedIndexSelection.java` | Walker for index selection |
| sirix-query `optimizer/` | `CostBasedStage.java` | Main cost-based optimization stage |
| test | `AdaptiveJoinOrderOptimizerTest.java` | DPhyp + linearizedDP + GOO-DP tests |
| test | `MeshTest.java` | Mesh data structure tests |

### Phase 5: Integration & Refinement (Weeks 9-10)

**Goal**: Wire into SirixOptimizer, end-to-end tests, benchmarks

**Files to Modify**:
| Location | File | Change |
|---|---|---|
| sirix-query `optimizer/` | `SirixOptimizer.java` | Add 3 new stages before IndexMatching |

**Files to Create**:
| Location | File | Description |
|---|---|---|
| test | `CostBasedOptimizationIntegrationTest.java` | End-to-end tests with real JSON data |
| test | `CostBasedBenchmarkTest.java` | Performance benchmarks |

### Phase 6: Advanced Features (Future)

- **Histogram support**: Build value histograms from CAS index B+-tree leaf scanning for better selectivity estimates (uncomment `IndexStatistics` in `IndexDef.java`)
- **Adaptive statistics**: Runtime feedback loop — actual vs estimated cardinalities, auto-adjust cost weights
- **Multi-version statistics**: Leverage SirixDB's temporal versioning for statistics across revisions
- **Parallel plan execution**: Block-based parallel evaluation using Brackit's `BlockCompileChain` + `Operator` interface

---

## 14. Performance Design Principles

### Memory Efficiency (HFT-style)
- **Primitive types** (`long`, `double`, `int`) throughout hot paths — no autoboxing
- **Object pooling** via `ThreadLocal` for `OperatorContext`
- **Pre-sized arrays** for DP tables in join ordering (16 relations = 65K entries × 3 arrays)
- **Fastutil primitive collections** for caches (`Object2LongOpenHashMap`, `Int2ObjectOpenHashMap`)
- **Mesh virtual edges** avoid copying shared subtrees between equivalence classes

### CPU Efficiency
- **DPhyp connected subgraph enumeration** — skips disconnected (invalid) subsets: O(n³) for chains vs O(2^n) for System R
- **Adaptive algorithm selection** via `countCC` — microsecond decision, avoids wasting time on exact DP for large queries
- **Neighborhood function** via bitmask OR — O(popcount) per call, cache-friendly
- **IKKBZ linearization** reduces search space from O(2^n) to O(n²) for medium queries
- **GOO-DP** greedy + bounded DP refinement — scales to 5,000+ relations (Neumann & Radke benchmarks)
- **Lazy initialization** of estimators (only when cost-based stage is reached)
- **Walker auto-restart** on modification (existing Brackit pattern) eliminates manual fixpoint loops

### Extensibility
- `CostModel` interface allows plugging custom cost models
- `StatisticsProvider` interface allows different statistics sources
- `PhysicalOperator` enum can be extended for new access methods
- Rewrite rules are independent walkers — new rules can be added without modifying existing code
- Mesh supports incremental updates — new plan alternatives are added without rebuilding

---

## 15. Critical Files Reference

### Brackit (Query Engine Framework)
1. **`brackit/.../optimizer/TopDownOptimizer.java`** — Base optimizer class, stage pipeline
2. **`brackit/.../optimizer/cost/CostModel.java`** — Existing cost model interface to extend
3. **`brackit/.../optimizer/cost/SimpleCostModel.java`** — Existing impl with JSON weights
4. **`brackit/.../optimizer/cost/OperatorContext.java`** — Existing context to extend
5. **`brackit/.../optimizer/walker/Walker.java`** — Base walker with auto-restart
6. **`brackit/.../optimizer/walker/topdown/ScopeWalker.java`** — Scope-aware walker base
7. **`brackit/.../optimizer/walker/topdown/JoinRewriter.java`** — Existing join detection

### SirixDB (Storage + Query Integration)
8. **`sirix-query/.../optimizer/SirixOptimizer.java`** — Main integration point
9. **`sirix-query/.../optimizer/walker/json/AbstractJsonPathWalker.java`** — JSON path analysis pattern
10. **`sirix-query/.../optimizer/walker/json/JsonCASStep.java`** — CAS index matching
11. **`sirix-query/.../optimizer/walker/json/JsonPathStep.java`** — Path index matching
12. **`sirix-query/.../optimizer/walker/json/JsonObjectKeyNameStep.java`** — Name index matching
13. **`sirix-core/.../index/path/summary/PathSummaryReader.java`** — Primary statistics source
14. **`sirix-core/.../index/path/summary/PathNode.java`** — Per-path cardinality (references)
15. **`sirix-core/.../index/IndexDef.java`** — Index definitions (has commented-out IndexStatistics)
16. **`sirix-core/.../api/NodeReadOnlyTrx.java`** — getChildCount(), getDescendantCount()
17. **`sirix-core/.../access/trx/node/IndexController.java`** — Index availability checks

---

## 16. Query Compilation & Vectorized Execution Engine

### 16.1 Overview: Three-Layer Architecture

The execution engine uses three layers, each optimizing at a different level.
Layers 1+2 work on any JVM (HotSpot, OpenJ9). Layer 3 is an optional GraalVM bonus.

```
┌────────────────────────────────────────────────────────────────────┐
│                    Query Execution Engine                          │
│                                                                    │
│  Layer 3: Truffle Pipeline Specialization (GraalVM only, optional)│
│  ─────────────────────────────────────────────────────────────    │
│  Truffle AST nodes for query operators. Partial evaluation fuses  │
│  entire pipelines into native code. Auto-deoptimizes on type      │
│  profile changes. Graceful fallback to Layer 1+2 on HotSpot.     │
│                                                                    │
│  Layer 2: Bytecode-Generated Pipeline Compilation                 │
│  ─────────────────────────────────────────────────────────────    │
│  ByteBuddy generates specialized Sink subclasses per pipeline.    │
│  Fuses Sink chains (ForBind→Select→GroupBy→Return) into single   │
│  compiled method. Eliminates virtual dispatch. Inlines type       │
│  checks. Works on any JVM — HotSpot C2 optimizes further.        │
│                                                                    │
│  Layer 1: Vectorized Batch Processing (SIMD)                      │
│  ─────────────────────────────────────────────────────────────    │
│  Columnar tuple batches (1024 tuples). Existing VectorOps.java   │
│  (627 lines) provides SIMD string comparison, numeric aggregation,│
│  filter evaluation, hash probing. MemorySegment zero-copy.        │
│                                                                    │
│  Foundation: Existing Brackit Block/Sink Push Model                │
│  ─────────────────────────────────────────────────────────────    │
│  Block.create(ctx, sink) → Sink.output(Tuple[], len)             │
│  ForkJoin parallelism, morsel-driven task splitting.              │
│  BlockCompileChain, BlockPipelineStrategy, BlockExpr.             │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 16.2 Current Execution Model (What We Have)

Brackit's existing block execution model is already well-designed for compilation:

```
Query → Parser → AST → Optimizer → BlockCompileChain
                                         │
                              BlockPipelineStrategy
                                         │
                              collectBlocks(AST)
                                         │
                              Block[] chain:
                                [ForBind, Select, GroupBy, OrderBy, ...]
                                         │
                              BlockExpr(BlockChain, returnExpr, ordered)
                                         │
                              FJControl.submit(EvalBlock)
                                         │
                              Sink.output(Tuple[], len) cascades down chain
```

**Key interfaces** (already exist):

| Interface | Role | Method |
|---|---|---|
| `Block` | Operator factory | `Sink create(QueryContext ctx, Sink sink)` |
| `Sink` | Push-based data flow | `void output(Tuple[] buf, int len)` |
| `Operator` | Pull-based cursor | `Cursor create(QueryContext ctx, Tuple[] tuples, int len)` |
| `Cursor` | Iterator | `Tuple next(QueryContext ctx)` |

**Existing SIMD** (`VectorOps.java`, 627 lines):
- `stringEquals()`, `stringCompare()`, `stringHash()` — vectorized UTF-8
- `sumLong()`, `sumDouble()`, `minLong()`, `maxLong()` — 4x unrolled vector accumulators
- `countGreaterThan()`, `filterIndicesGreaterThan()` — vectorized predicates
- `hashProbe()` — vectorized linear probing for hash joins

### 16.3 Layer 1: Columnar Vectorized Batch Processing

#### From Row Batches to Columnar Batches

The current `Sink.output(Tuple[], len)` passes **row-oriented** tuple arrays.
For SIMD to be fully effective, we need **columnar** representation within batches:

```java
/**
 * Columnar batch of tuples for vectorized processing.
 * Each column is a contiguous primitive array — SIMD-friendly.
 *
 * Location: brackit/block/ColumnBatch.java
 */
public final class ColumnBatch {

    // Batch size — chosen for L1 cache residency (64KB / ~64 bytes per tuple)
    public static final int BATCH_SIZE = 1024;

    // Column storage — one array per attribute
    private final long[][] longColumns;       // numeric columns (int/long/double bits)
    private final byte[][] stringColumns;     // UTF-8 string data (packed)
    private final int[][] stringOffsets;       // offset+length pairs into string data
    private final boolean[][] nullBitmaps;     // null tracking per column

    // Selection vector: indices of "alive" tuples after filtering
    // Avoids copying — just marks which rows survived a predicate
    private final int[] selectionVector;
    private int selectedCount;

    // Metadata
    private final int columnCount;
    private int rowCount;

    public ColumnBatch(int columnCount) {
        this.columnCount = columnCount;
        this.longColumns = new long[columnCount][BATCH_SIZE];
        this.stringColumns = new byte[columnCount][];
        this.stringOffsets = new int[columnCount][];
        this.nullBitmaps = new boolean[columnCount][BATCH_SIZE];
        this.selectionVector = new int[BATCH_SIZE];
        this.selectedCount = 0;
        this.rowCount = 0;
    }

    /** Set row count with bounds check to prevent buffer overflows. */
    public void setRowCount(int count) {
        if (count < 0 || count > BATCH_SIZE) {
            throw new IllegalArgumentException(
                "rowCount " + count + " out of range [0, " + BATCH_SIZE + "]");
        }
        this.rowCount = count;
    }

    /**
     * Apply a filter predicate to a long column using SIMD.
     * Updates the selection vector in-place — no data copying.
     */
    public void filterGreaterThan(int columnIdx, long threshold) {
        selectedCount = VectorOps.filterIndicesGreaterThan(
            longColumns[columnIdx], threshold, selectionVector, rowCount);
    }

    /**
     * Compute vectorized aggregation on a column, respecting selection vector.
     */
    public long sumLong(int columnIdx) {
        if (selectedCount == rowCount) {
            // No filter active — full SIMD scan
            return VectorOps.sumLong(longColumns[columnIdx], 0, rowCount);
        }
        // Filtered — gather selected values then SIMD sum
        return sumLongSelected(longColumns[columnIdx], selectionVector, selectedCount);
    }

    // ... additional vectorized operations
}
```

#### Vectorized Sink Interface Extension

```java
/**
 * Extended Sink that processes columnar batches.
 * Falls back to row-oriented output() for non-vectorized operators.
 *
 * Location: brackit/block/VectorizedSink.java
 */
public interface VectorizedSink extends Sink {

    /**
     * Process a columnar batch. Default implementation converts
     * to Tuple[] and delegates to output(Tuple[], len).
     */
    default void outputBatch(ColumnBatch batch) {
        // Fallback: materialize to row tuples
        Tuple[] tuples = batch.materializeRows();
        output(tuples, batch.getRowCount());
    }

    /**
     * Whether this sink supports columnar batch processing.
     * Compiled sinks return true; interpreted sinks return false.
     */
    default boolean supportsColumnBatch() {
        return false;
    }
}
```

#### Vectorized Operator Implementations

```java
/**
 * Vectorized Select (WHERE clause) — processes entire column batches
 * using SIMD predicates instead of per-tuple evaluation.
 *
 * Location: brackit/block/VectorizedSelect.java
 */
public final class VectorizedSelect implements Block {

    private final int columnIdx;           // which column to filter
    private final ComparisonOp op;         // EQ, GT, LT, GE, LE, NE
    private final long constantValue;      // comparison constant (long-encoded)
    private final boolean hasConstantRHS;  // true if RHS is a compile-time constant

    @Override
    public Sink create(QueryContext ctx, Sink downstream) {
        if (hasConstantRHS && downstream instanceof VectorizedSink vs
                && vs.supportsColumnBatch()) {
            return new VectorizedSelectSink(vs, columnIdx, op, constantValue);
        }
        // Fallback to interpreted Select
        return new InterpretedSelectSink(downstream, columnIdx, op, constantValue);
    }
}

final class VectorizedSelectSink implements VectorizedSink {
    private final VectorizedSink downstream;
    private final int columnIdx;
    private final ComparisonOp op;
    private final long constant;

    @Override
    public void outputBatch(ColumnBatch batch) {
        // SIMD predicate evaluation — no per-tuple virtual dispatch
        switch (op) {
            case GT -> batch.filterGreaterThan(columnIdx, constant);
            case EQ -> batch.filterEquals(columnIdx, constant);
            case LT -> batch.filterLessThan(columnIdx, constant);
            // ...
        }
        downstream.outputBatch(batch); // pass filtered batch (selection vector updated)
    }

    @Override
    public boolean supportsColumnBatch() { return true; }
}
```

### 16.4 Layer 2: Bytecode-Generated Pipeline Compilation

#### The Produce/Consume Model (Neumann, VLDB 2011)

HyPer's key insight: instead of operators calling each other through virtual dispatch,
**compile the entire pipeline into a single method**. Each operator contributes code fragments:

- `produce()` — "start generating tuples" (push data downstream)
- `consume(tuple)` — "here's a tuple from my child" (process and push further)

At pipeline breakers (hash build, sort, materialize), the compiled pipeline ends and
a new pipeline begins. Between breakers, there are **zero virtual calls** in the inner loop.

#### Pipeline Breakers in Brackit

```
Pipeline 1 (scan + filter):           Pipeline 2 (probe + aggregate):
  ForBind.produce()                      TableJoin.probePhase.produce()
    → Select.consume(tuple)                → GroupBy.consume(tuple)
      → TableJoin.buildPhase.consume()       → Return.consume(tuple)
         ← BREAKER: hash table build          ← BREAKER: result materialization
```

#### Bytecode Generation via ByteBuddy

```java
/**
 * Compiles a Sink chain into a single specialized class via ByteBuddy.
 * The generated class fuses all operators between pipeline breakers
 * into one tight method with no virtual dispatch.
 *
 * Location: brackit/compiler/jit/PipelineCompiler.java
 */
public final class PipelineCompiler {

    // Compilation threshold — don't compile cold pipelines
    private static final int HOT_THRESHOLD = 1000;

    // Cache of compiled pipelines (query plan hash → compiled class)
    private final ClassValue<Class<?>> compiledPipelines = new ClassValue<>() {
        @Override
        protected Class<?> computeValue(Class<?> type) { return null; }
    };

    /**
     * Attempt to compile a Block chain into fused bytecode.
     * Returns a CompiledBlockExpr if successful, null if not compilable.
     */
    public BlockExpr tryCompile(Block[] chain, Expr returnExpr, boolean ordered) {
        // Analyze the chain for pipeline breakers
        List<PipelineSegment> segments = findPipelineSegments(chain);

        // Generate bytecode for each segment
        List<Class<? extends Sink>> compiledSegments = new ArrayList<>();
        for (PipelineSegment segment : segments) {
            Class<? extends Sink> compiled = compileSegment(segment);
            if (compiled == null) return null; // bail out if any segment not compilable
            compiledSegments.add(compiled);
        }

        return new CompiledBlockExpr(compiledSegments, returnExpr, ordered);
    }

    /**
     * Compile a pipeline segment (chain of non-breaking operators) into
     * a single Sink class with fused output() method.
     */
    private Class<? extends Sink> compileSegment(PipelineSegment segment) {
        // Example: ForBind → Select(price > 40) → Project(price)
        // Generates bytecode equivalent to:
        //
        //   void output(Tuple[] buf, int len) {
        //     for (int i = 0; i < len; i++) {
        //       long price = buf[i].getLong(3);        // ForBind: extract binding
        //       if (price > 40L) {                      // Select: inlined predicate
        //         outBuf[outIdx++] = buf[i].project(3); // Project: inlined projection
        //         if (outIdx == BATCH_SIZE) {
        //           downstream.output(outBuf, outIdx);
        //           outIdx = 0;
        //         }
        //       }
        //     }
        //   }

        return new ByteBuddy()
            .subclass(AbstractSink.class)
            .name("CompiledPipeline$" + segment.hashCode())
            .method(named("output"))
            .intercept(new FusedOutputImplementation(segment))
            .make()
            .load(getClass().getClassLoader())
            .getLoaded();
    }

    /**
     * Identify pipeline breakers that force materialization.
     * Between breakers, all operators can be fused into one method.
     */
    private List<PipelineSegment> findPipelineSegments(Block[] chain) {
        List<PipelineSegment> segments = new ArrayList<>();
        List<Block> current = new ArrayList<>();

        for (Block block : chain) {
            if (isPipelineBreaker(block)) {
                if (!current.isEmpty()) {
                    segments.add(new PipelineSegment(current));
                    current = new ArrayList<>();
                }
                // Breaker itself starts a new segment
                segments.add(new PipelineSegment(List.of(block)));
            } else {
                current.add(block);
            }
        }

        if (!current.isEmpty()) {
            segments.add(new PipelineSegment(current));
        }
        return segments;
    }

    private boolean isPipelineBreaker(Block block) {
        // Pipeline breakers require full materialization before proceeding
        return block instanceof TableJoin     // hash build phase
            || block instanceof OrderBy       // must see all tuples before sorting
            || block instanceof GroupBy;      // hash aggregation
    }
}

record PipelineSegment(List<Block> operators) {}
```

#### Adaptive Compilation: Interpret First, Compile Hot Paths

```java
/**
 * BlockExpr that adaptively switches from interpretation to compiled execution.
 * Counts invocations; once hot, compiles and replaces itself.
 *
 * Following Umbra's approach: interpret vectorized for cold paths,
 * compile for hot paths.
 *
 * Location: brackit/expr/AdaptiveBlockExpr.java
 */
public final class AdaptiveBlockExpr extends BlockExpr {

    private static final int COMPILATION_THRESHOLD = 1000;
    private final PipelineCompiler compiler;
    private volatile BlockExpr compiledVersion;   // lazily compiled
    private int invocationCount;                  // no need for atomic — approximate is fine

    @Override
    public Sequence evaluate(QueryContext ctx, Tuple tuple) {
        // Fast path: already compiled
        if (compiledVersion != null) {
            return compiledVersion.evaluate(ctx, tuple);
        }

        // Count invocations; compile when hot
        if (++invocationCount >= COMPILATION_THRESHOLD) {
            BlockExpr compiled = compiler.tryCompile(getBlocks(), getReturnExpr(), isOrdered());
            if (compiled != null) {
                compiledVersion = compiled;
                return compiled.evaluate(ctx, tuple);
            }
            // Compilation failed — stop trying
            invocationCount = Integer.MIN_VALUE;
        }

        // Interpreted execution (existing BlockExpr behavior)
        return super.evaluate(ctx, tuple);
    }
}
```

#### What Gets Compiled vs. Interpreted

| Operator | Compilable? | Compilation Strategy |
|---|---|---|
| `ForBind` (scan) | Yes | Inline iterator, fuse with downstream |
| `Select` (WHERE) | Yes | Inline predicate, vectorize if constant RHS |
| `LetBind` | Yes | Inline binding expression |
| `Project` (RETURN) | Yes | Inline field extraction |
| `TableJoin` (hash) | **Breaker** | Compile build phase and probe phase as separate pipelines |
| `GroupBy` | **Breaker** | Compile aggregation accumulation kernel |
| `OrderBy` | **Breaker** | Not compiled — delegates to Arrays.sort() |
| `NLJoin` | Partial | Compile inner loop body only |

### 16.5 Layer 3: Optional GraalVM Truffle Acceleration

When running on GraalVM, we can additionally leverage Truffle's partial evaluation
for deeper pipeline specialization. This is **optional** — Layers 1+2 provide the
primary performance benefit on any JVM.

#### Truffle Node Hierarchy for Query Operators

```java
/**
 * Base Truffle node for query operators.
 * Truffle's partial evaluation will specialize and inline
 * the entire operator tree into native code.
 *
 * Location: brackit/truffle/node/QueryOperatorNode.java
 * Only compiled/loaded when GraalVM is detected at startup.
 */
@NodeInfo(shortName = "QueryOp")
public abstract class QueryOperatorNode extends Node {

    /**
     * Execute this operator, producing output tuples.
     * Truffle specializes based on observed types and profiles.
     */
    public abstract void execute(VirtualFrame frame, ColumnBatch batch);
}

/**
 * Truffle-specialized Select node.
 * Uses @Specialization for type-specific fast paths.
 */
@NodeInfo(shortName = "Select")
public abstract class TruffleSelectNode extends QueryOperatorNode {

    @Child private QueryOperatorNode downstream;
    private final int columnIdx;

    /**
     * Specialization for long comparison with constant RHS.
     * Truffle compiles this to a tight native loop with no type checks.
     */
    @Specialization(guards = "isLongColumn(batch, columnIdx)")
    protected void doLongConstant(VirtualFrame frame, ColumnBatch batch,
                                   @Cached("getConstant()") long constant) {
        // SIMD filter — Truffle inlines VectorOps call into compiled code
        batch.filterGreaterThan(columnIdx, constant);
        downstream.execute(frame, batch);
    }

    /**
     * Specialization for string comparison.
     */
    @Specialization(guards = "isStringColumn(batch, columnIdx)")
    protected void doString(VirtualFrame frame, ColumnBatch batch) {
        // String-specific filtering path
        batch.filterStringEquals(columnIdx, patternBytes);
        downstream.execute(frame, batch);
    }

    /**
     * Generic fallback — interpreted evaluation.
     */
    @Fallback
    protected void doGeneric(VirtualFrame frame, ColumnBatch batch) {
        // Row-by-row interpreted filtering
        for (int i = 0; i < batch.getRowCount(); i++) {
            if (evaluatePredicate(batch, i)) {
                batch.select(i);
            }
        }
        downstream.execute(frame, batch);
    }
}
```

#### Truffle CallTarget for Complete Pipelines

```java
/**
 * Wraps a compiled pipeline as a Truffle CallTarget.
 * Truffle's compilation infrastructure handles:
 * - Profiling-guided specialization
 * - Deoptimization on type profile changes
 * - OSR (On-Stack Replacement) for long-running loops
 * - Inlining across operator boundaries
 *
 * Location: brackit/truffle/TrufflePipelineCallTarget.java
 */
public final class TrufflePipelineCallTarget {

    private final RootNode pipelineRoot;
    private final CallTarget callTarget;

    public TrufflePipelineCallTarget(QueryOperatorNode[] operators) {
        this.pipelineRoot = new PipelineRootNode(operators);
        this.callTarget = Truffle.getRuntime().createCallTarget(pipelineRoot);
    }

    public void execute(ColumnBatch batch) {
        callTarget.call(batch);
    }
}
```

#### GraalVM Detection and Graceful Fallback

```java
/**
 * Factory that selects the appropriate execution strategy
 * based on the available JVM capabilities.
 *
 * Location: brackit/compiler/ExecutionStrategyFactory.java
 */
public final class ExecutionStrategyFactory {

    private static final boolean GRAALVM_AVAILABLE = detectGraalVM();
    private static final boolean VECTOR_API_AVAILABLE = detectVectorAPI();

    /**
     * Create the best available BlockExpr for the given pipeline.
     */
    public static BlockExpr createBlockExpr(Block[] chain, Expr returnExpr, boolean ordered) {
        if (GRAALVM_AVAILABLE) {
            // Layer 3: Truffle pipeline specialization
            return new TruffleBlockExpr(chain, returnExpr, ordered);
        }

        if (VECTOR_API_AVAILABLE) {
            // Layer 2 + 1: Adaptive compilation + vectorized batches
            return new AdaptiveBlockExpr(chain, returnExpr, ordered, new PipelineCompiler());
        }

        // Fallback: existing interpreted BlockExpr
        return new BlockExpr(chain, returnExpr, ordered);
    }

    private static boolean detectGraalVM() {
        try {
            Class.forName("com.oracle.truffle.api.Truffle");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static boolean detectVectorAPI() {
        try {
            Class.forName("jdk.incubator.vector.LongVector");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
```

### 16.6 How the Three Layers Interact

#### Example: `for $i in $doc.items[] where $i.price > 40 return $i.price`

**Interpreted execution** (current, ~100ns/tuple):
```
ForBind.create(ctx, selectSink)
  → selectSink.output(tuples, len)           // virtual dispatch
    → for each tuple:
        if tuple.evaluate(predicate) > 40:    // virtual dispatch × 3 (deref, compare, box)
          returnSink.output(...)              // virtual dispatch
```

**Layer 1 — Vectorized** (~15ns/tuple):
```
ForBind → VectorizedSelectSink.outputBatch(columnBatch)
  → batch.filterGreaterThan(priceColumn, 40L)   // SIMD: 16 longs compared per instruction
  → downstream.outputBatch(batch)                // selection vector, no data copy
```

**Layer 2 — Compiled + Vectorized** (~5ns/tuple):
```
CompiledPipeline$xxx.output(Tuple[] buf, int len):
  // Single method — no virtual dispatch, all inlined by HotSpot C2
  long[] prices = extractLongColumn(buf, 3, len);    // batch extract
  int[] selected = VectorOps.filterIndicesGreaterThan(prices, 40L, len);
  for (int i = 0; i < selected.length; i++) {
    outBuf[outIdx++] = prices[selected[i]];          // direct long, no boxing
  }
  downstream.output(outBuf, outIdx);                  // known monomorphic call
```

**Layer 3 — Truffle on GraalVM** (~2-3ns/tuple):
```
// Truffle partial evaluation compiles entire pipeline to native code:
// - ForBind scan loop
// - Select predicate (specialized for long > constant)
// - Return projection (field extraction)
// All fused into one native function with zero intermediate allocations.
// Truffle additionally:
// - Eliminates bounds checks via @CompilerDirectives.transferToInterpreterAndInvalidate()
// - Profiles branch probabilities for predicate selectivity
// - Applies OSR for long-running scan loops
```

### 16.7 Integration with Cost-Based Optimizer

The cost model (Section 7) should account for the execution layer:

```java
// In JsonCostModel — adjust costs based on execution capability
private double adjustForExecutionLayer(double baseCost, OperatorContext ctx) {
    if (ExecutionStrategyFactory.isGraalVMAvailable()) {
        return baseCost * 0.3;   // Truffle compilation ~3x faster than interpreted
    }
    if (ExecutionStrategyFactory.isVectorAPIAvailable()) {
        return baseCost * 0.5;   // SIMD vectorization ~2x faster
    }
    return baseCost;
}
```

The optimizer can also make **compilation-aware decisions**:
- Prefer hash joins over nested-loop joins (hash build is a pipeline breaker, but probe is compilable)
- Prefer columnar projections pushed down early (fewer columns = more tuples fit in SIMD batch)
- Avoid late materialization when it prevents vectorization

### 16.8 Implementation Phases for Compilation Engine

#### Phase A: Columnar Batches (Weeks 1-2)

**Goal**: Introduce `ColumnBatch` alongside existing `Tuple[]`, wire into existing SIMD `VectorOps`

| Location | File | Description |
|---|---|---|
| brackit `block/` | `ColumnBatch.java` | Columnar batch with selection vectors |
| brackit `block/` | `VectorizedSink.java` | Extended Sink interface for batch processing |
| brackit `block/` | `VectorizedSelect.java` | SIMD-accelerated WHERE clause |
| brackit `block/` | `VectorizedForBind.java` | Batch-oriented scan with columnar output |
| test | `ColumnBatchTest.java` | SIMD filter/aggregate correctness tests |

#### Phase B: Pipeline Compilation (Weeks 3-5)

**Goal**: ByteBuddy-based pipeline fusion for hot query paths

| Location | File | Description |
|---|---|---|
| brackit `compiler/jit/` | `PipelineCompiler.java` | Bytecode generation for Sink chain fusion |
| brackit `compiler/jit/` | `PipelineSegment.java` | Identifies pipeline breakers, groups fusible operators |
| brackit `compiler/jit/` | `FusedOutputImplementation.java` | ByteBuddy MethodDelegation for fused output() |
| brackit `expr/` | `AdaptiveBlockExpr.java` | Interpret-first, compile-when-hot adaptive executor |
| brackit `compiler/` | `ExecutionStrategyFactory.java` | Runtime capability detection, strategy selection |
| test | `PipelineCompilerTest.java` | Compilation correctness + performance benchmarks |

#### Phase C: Truffle Integration (Weeks 6-8, optional)

**Goal**: GraalVM Truffle nodes for deep pipeline specialization

| Location | File | Description |
|---|---|---|
| brackit `truffle/node/` | `QueryOperatorNode.java` | Base Truffle node for query operators |
| brackit `truffle/node/` | `TruffleSelectNode.java` | Specialized Select with @Specialization |
| brackit `truffle/node/` | `TruffleForBindNode.java` | Specialized scan with OSR support |
| brackit `truffle/node/` | `TruffleHashJoinNode.java` | Specialized hash join build+probe |
| brackit `truffle/` | `TrufflePipelineCallTarget.java` | CallTarget wrapper for compiled pipelines |
| brackit `truffle/` | `TruffleBlockExpr.java` | BlockExpr using Truffle compilation |
| brackit `truffle/` | `TruffleLanguage.java` | Truffle language registration (required by framework) |
| test | `TrufflePipelineTest.java` | End-to-end Truffle compilation tests |

### 16.9 Performance Expectations

Based on published benchmarks from comparable systems:

| Execution Mode | Throughput (tuples/sec) | Latency (ns/tuple) | Reference System |
|---|---|---|---|
| Interpreted (current Brackit) | ~10M | ~100 | — |
| Vectorized (Layer 1) | ~60M | ~15 | DuckDB, Velox |
| Compiled (Layer 2) | ~200M | ~5 | Spark Tungsten |
| Truffle-compiled (Layer 3) | ~300-500M | ~2-3 | GraalDB, Umbra-class |

These are order-of-magnitude estimates for simple scan+filter+aggregate pipelines.
Complex queries with joins and multiple pipeline breakers will see smaller improvements,
but the compilation still eliminates virtual dispatch overhead at every operator boundary.

---

## 17. End-to-End Example

### Query
```jsoniq
let $doc := jn:doc('auction','auctions.jn')
return count(
  for $i in $doc.site.closed_auctions[]
  where $i.price > 40
  return $i.price
)
```

### Optimization Trace

**Step 1: Analysis** → JQGM Query Graph
```
DOCUMENT ACCESS($doc) →
  OBJECT ACCESS(.site) →
    OBJECT ACCESS(.closed_auctions) →
      ARRAY ACCESS([]) →
        SELECT(.price > 40) →
          OBJECT ACCESS(.price) →
            SELECT(count()) → out
```

**Step 2: JqgmRewriteStage**

Rule 1 (Join Fusion): Fuse `.site → .closed_auctions → [] → .price` into single 4-way join
```
JOIN_4WAY($doc.site.closed_auctions[].price)
  ├── SELECT(.price > 40)   ← Rule 2: fused into join
  └── SELECT(count()) → out
```

Rule 3 (Select + Access Fusion): Fuse `price > 40` into OBJECT ACCESS for `.price`
```
JOIN_4WAY($doc.site.closed_auctions[].price)
  with predicate: price > 40
  └── SELECT(count()) → out
```

**Step 3: CostBasedStage**

Statistics collection (from PathSummary):
- `$doc.site.closed_auctions[]` → 10,000 nodes (PathNode.getReferences())
- `$doc.site.closed_auctions[].price` → 10,000 nodes
- Total document: 500,000 nodes

Cost comparison for `.price > 40`:
- Sequential scan: 500,000 × 1.0 = **500,000**
- CAS Index scan (if exists on `price`): 10 + (3 × 10.0) + (3,300 × 0.08) = **304** ← winner
- Path Index scan (if exists): 10 + (3 × 10.0) + (10,000 × 0.1) = **1,040**

→ Annotate: `preferIndex=true, indexId=2, indexType=CAS`

**Step 4: IndexDecompositionStage**

Rule 5: CAS index `I₂(//price[Number])` covers `.price > 40` subtree
→ Split join: index scan for `.price > 40` + navigation for `.site.closed_auctions[]`

**Step 5: IndexMatching** (existing)

`JsonCASStep` sees `preferIndex=true` annotation → rewrites to CAS B+-tree range scan

**Final QEP**:
```
CAS_INDEX_SCAN(price > 40, indexId=2)   cost: 304
  → CURSOR_NAVIGATE(.site.closed_auctions[])  cost: 200
    → COUNT_AGGREGATE                           cost: 1
```
Total estimated cost: **505** (vs 500,000 for full scan — **990× improvement**)

---

## 18. Formal Correctness Properties

### 18.1 DPhyp Completeness and Soundness

**Theorem 1 (Completeness)**: DPhyp enumerates all valid join trees for a connected query graph G = (V, E).

*Proof sketch (Moerkotte & Neumann, SIGMOD 2008, Section 3)*:

Every join tree T for G can be decomposed into a root join T₁ ⋈ T₂ where:
- T₁ and T₂ are both connected subgraphs of G
- There exists at least one edge (r₁, r₂) ∈ E with r₁ ∈ T₁, r₂ ∈ T₂
- T₁ and T₂ partition V(T) (the vertices of T)

Such a pair (T₁, T₂) is a *connected subgraph complement pair (csg-cmp-pair)*. DPhyp's `EnumerateCsgRec` generates all connected subgraphs S by starting from each node vᵢ and expanding through its neighborhood N(S). **Connectivity preservation**: if S is connected and S' ⊆ N(S), then S ∪ S' is connected (every node in S' has an edge to some node in S by definition of N). For each connected S₁, `EmitCsg` generates all connected complements S₂ adjacent to S₁ via `EnumerateCmpRec`, which performs per-subset recursion so that each incrementally expanded complement is itself expanded through its own neighborhood. Since every valid join decomposition corresponds to a csg-cmp-pair, and DPhyp enumerates all such pairs, it considers every valid join tree. ∎

**Theorem 2 (Soundness)**: DPhyp only produces joins backed by join predicates. GOO-DP may additionally introduce Cartesian products only for disconnected query graph components.

*Proof*: In the DPhyp path, `EmitCsgCmp` only considers pairs (S₁, S₂) where S₂ is reached through `getNeighborhood(S₁)`. By definition of the neighborhood function, there must exist an edge in E connecting S₁ to S₂. Therefore, every join produced by DPhyp is backed by a join predicate. In the GOO-DP path (for very large or disconnected graphs), the greedy phase introduces Cartesian products only when `hasJoinPredicate(sets[l], sets[r])` returns false for all pairs — meaning the remaining components are genuinely disconnected. This is necessary for completeness: a valid query plan must connect all relations. ∎

**Theorem 3 (Optimality)**: Given a cost function C satisfying the Bellman principle (optimal substructure), DPhyp produces the minimum-cost join tree.

*Proof*: DPhyp is a dynamic programming algorithm. For each csg-cmp-pair (S₁, S₂), it evaluates C(S₁ ⋈ S₂) = C(S₁) + C(S₂) + joinCost(S₁, S₂), keeping only the minimum. Both orderings (S₁ ⋈ S₂ and S₂ ⋈ S₁) are evaluated in `emitCsgCmp` since the join cost function is asymmetric (build cost ≠ probe cost). Since all pairs are enumerated (Theorem 1), both orderings are considered, and the Bellman principle guarantees that the optimal tree for S₁ ∪ S₂ decomposes into optimal subtrees for S₁ and S₂, the final result is globally optimal. ∎

### 18.2 Cost Function Bellman Principle

Our cost function satisfies optimal substructure (additive separability):

```
C(R₁ ⋈ R₂) = C(R₁) + C(R₂) + joinCost(|R₁|, |R₂|)
```

where `joinCost` depends only on the cardinalities of R₁ and R₂ (not on their internal structure). This follows the `C_sum` formulation from [Moerkotte & Neumann 2006].

**Conditions for the Bellman principle:**
1. **Additive separability**: `C(S₁ ∪ S₂) = C(S₁) + C(S₂) + joinCost(|S₁|, |S₂|)` — the cost of a union depends only on the costs and cardinalities of its parts. ✓ (our cost model satisfies this by construction)
2. **Cardinality-only dependency**: `joinCost` must depend only on `|R₁|` and `|R₂|`, not on their internal plan structure. ✓ (`estimateJoinCost(probeCard, ctx)` reads only `ctx.inputCardinality` and `probeCard`; all other `OperatorContext` fields remain at defaults during DPhyp join ordering)
3. **Monotonicity**: `|R₁'| > |R₁| ⟹ joinCost(|R₁'|, |R₂|) ≥ joinCost(|R₁|, |R₂|)`. ✓ (both `buildCost = |R₁| × 1.5` and `probeCost = |R₂| × 0.5` are linear, hence monotone)

**Fragility note**: If future code sets `OperatorContext` fields (e.g., `ioWeight`, `cpuWeight`) before `estimateJoinCost` during the DPhyp inner loop, the cardinality-only dependency (condition 2) would break. The join ordering code path must preserve this invariant.

### 18.3 Rewrite Rule Correctness

Each rewrite rule preserves query semantics (produces the same result set):

**Rule 1 (Join Fusion)**: Fusing two binary joins `(A ⋈_p B) ⋈_q C` into a 3-way join `A ⋈_{p∧q} (B, C)` is correct when both predicates are structural (navigational). *Proof*: This is not simple associativity — it is a conversion from binary to n-ary join. The n-ary join evaluates the conjunction p ∧ q over all tuple variables simultaneously. Since structural predicates are independent path constraints (each tests a specific parent-child or ancestor-descendant relationship), evaluating them conjunctively produces the same result as evaluating them sequentially: `σ_p(σ_q(R)) = σ_{p∧q}(R)` for independent predicates. *Precondition*: both p and q must be structural predicates (no value-dependent cross-references between the two join predicates).

**Rule 2 (Select-Join Fusion)**: Moving a selection σ_p into a join ⋈_q produces `⋈_{p∧q}`, which filters the same tuples. *Proof*: `σ_p(R ⋈_q S) = R ⋈_{p∧q} S` when p only references attributes of R (standard selection pushdown equivalence from relational algebra). *Precondition*: p must reference only attributes of R (not S), and must not contain L-quantified (let-bound) variables that depend on the join's output.

**Rule 3 (Select-Access Fusion)**: Moving a selection into an access operator is correct because the access operator can evaluate the predicate during node retrieval. *Proof*: `σ_p(ACCESS(path)) = ACCESS(path, p)`. Selection is order-preserving: removing tuples from an ordered sequence preserves the relative order of remaining tuples.

**Rule 4 (Commutativity)**: `R ⋈_p S = S ⋈_{p'} R` where p' reverses the axis direction. Holds for child-key (reverse: parent) and descendant-key (reverse: ancestor). Does NOT hold for child-index (array position has no reverse). *Proof*: For structural predicates, `ancestor(a, d) ⟺ descendant(d, a)` and `parent(p, c) ⟺ child(c, p)`. Array indices are inherently directional: `arr[i]` has no inverse.

**Rules 5-6 (Decomposition)**: Splitting a join at an index boundary produces two subqueries whose intersection equals the original result, **provided the index covers the exact same node set**.

*Preconditions* (must be verified by the index matching logic):
- **CAS index**: The index must cover the path *p* with a type that is a superset of the query's type restriction. A CAS index on `price[Number]` cannot substitute for a scan over all `price` values (which may include strings). The index decomposition must include a type-compatibility check.
- **Path index**: The index path must exactly match the query path. A path index on `/a/b` does not cover `/a/c/b`.
- **Name index**: A name index on `"price"` returns ALL nodes named "price" regardless of path. It can only substitute for a scan when the query also has no path restriction, or when a path filter is added as a post-filter to the index scan.

*Proof*: When the preconditions hold, `ACCESS_I(p) = ACCESS_SCAN(p)` because the index produces the exact same node set as a navigational scan. Splitting the join at the index boundary then decomposes the query into an index-accelerated subquery and a navigational subquery whose joined results equal the original unsplit query result (by the intersection property of structural joins).

### 18.4 Cardinality Estimation Consistency

The cardinality estimator preserves the following invariants:

1. **Non-negativity**: `estimateCardinality(op) ≥ 1` for all operators (enforced by `Math.max(1L, ...)`)
2. **Monotonicity under selection**: `card(σ_p(R)) ≤ card(R)` since `0 < selectivity ≤ 1`
3. **ForBind multiplication**: `card(for $x in R return f($x)) = card(R) × card(f)` — correct for independent bindings
4. **LetBind preservation**: `card(let $x := e return f($x)) = card(f)` — let does not multiply tuples
5. **Join bound**: `card(R ⋈ S) ≤ card(R) × card(S)` since `0 < joinSelectivity ≤ 1`

### 18.5 Adaptive Strategy Correctness

**Theorem 4**: The adaptive strategy produces optimal plans for queries with < 14 relations.

*Proof*: For |V| < 14, the adaptive strategy unconditionally uses DPhyp. By Theorems 1-3, DPhyp produces the optimal plan. ∎

**Theorem 5**: The adaptive strategy terminates for all query sizes.

*Proof*:
- **DPhyp**: terminates because the number of csg-cmp-pairs is finite (bounded by 3^n, Moerkotte & Neumann 2008). The subset enumeration loops use Gosper's bit trick `(subset - 1) & neighborhood`, which strictly decreases `subset` in each iteration. The DpTable throws `IllegalStateException` if MAX_CAPACITY is exceeded, converting potential non-termination into a fail-fast error.
- **LinearizedDP**: terminates because it consists of three nested loops with fixed bounds (chain length ≤ n, start position ≤ n-len, split point < chain length). Total iterations: O(n³).
- **GOO-DP**: The greedy phase reduces `poolSize` by 1 each iteration (including the disconnected-graph fallback), terminating in exactly n-1 steps. The DP refinement phase iterates over a finite `candidates` list. For each candidate, either (a) `dpHyp` succeeds and budget is reduced by at least 3^2=9, or (b) `dpHyp` returns null and the loop advances to the next candidate. In either case, the for-each loop terminates because the candidates list is finite and unmodified during iteration.
∎

### 18.6 Vectorized Execution Equivalence

**Theorem 6**: Vectorized batch execution produces the same results as tuple-at-a-time execution.

*Proof*: The selection vector is the only optimization — it avoids data copying but does not change which tuples survive filtering. For a predicate p and batch B:
- Tuple-at-a-time: `{t ∈ B | p(t) = true}`
- Vectorized: `{B[i] | selectionVector[i] ∈ filterIndices(B, p)}`

These produce the same set because `filterIndices` returns exactly the indices where p evaluates to true. SIMD operations (VectorOps) compute the same boolean results as scalar operations — they differ only in throughput, not semantics:
- **Integer operations**: Exact equivalence (addition, comparison are commutative/associative in two's complement).
- **Comparison operations**: The Java Vector API (JEP 338) mandates IEEE 754 semantics for all comparison operations, including NaN handling: `NaN > x`, `NaN == x`, etc. all evaluate to false, identically to scalar Java semantics.
- **Floating-point aggregations**: SIMD may reorder additions (e.g., `(a+b)+(c+d)` vs `((a+b)+c)+d`). Due to IEEE 754 non-associativity, results may differ by small rounding errors. For integer aggregations (`sumLong`), equivalence is exact. ∎

### 18.7 Pipeline Compilation Equivalence

**Theorem 7**: Compiled pipelines produce the same results as interpreted pipelines.

*Proof*: ByteBuddy-generated `Sink` subclasses implement the same `output(Tuple[], len)` interface. The fused method body is a **source-level code sequencing** of the individual Sink bodies:

```
// Interpreted: sink1.output → sink2.output → sink3.output (virtual dispatch chain)
// Compiled: all three bodies concatenated in one method (direct execution)
```

The equivalence holds because: (1) the method bodies are identical in both versions, (2) they execute in the same sequential order, (3) no observable side effects depend on call stack depth or virtual dispatch metadata (JLS §15.12 — method invocation semantics are defined by the method body, not the dispatch mechanism). The Java Memory Model (JLS §17.4) guarantees that correctly-synchronized programs produce the same results regardless of inlining or compilation optimizations. **Assumption**: no operator uses exceptions for non-local flow control (exception stack traces will differ between interpreted and compiled paths, but this is non-observable for result correctness). ∎

### 18.8 Rewrite Rule Termination

**Theorem 8**: The `JqgmRewriteStage` fixpoint loop terminates.

*Proof*: The loop is bounded by `iterations < 10` (hard limit). Within each iteration, four walkers are applied. Each walker returns the same AST reference if no modification was made (Brackit Walker contract), and the `modified` flag tracks whether any walker produced a change. The loop exits either when `modified == false` (fixpoint reached) or when 10 iterations are exceeded (hard bound).

**Note on potential oscillation**: Rule 4 (Commutativity) could theoretically swap a join, causing Rules 1-3 to re-trigger, producing a cycle. The 10-iteration bound guarantees termination but not convergence. In practice, Rule 4 only fires once per join (commutativity is idempotent: swapping twice returns to the original), and Rules 1-3 are monotonically reducing (each fusion removes a node from the AST). Therefore, the expected number of iterations is ≤ 3 for typical queries.

### 18.9 GOO-DP Refinement Monotonicity

**Theorem 9**: GOO-DP refinement never increases total plan cost.

*Proof*: Each refinement call replaces a join subtree with the result of `dpHyp` on the same set of relations. By Theorem 3, `dpHyp` produces the optimal (minimum-cost) plan for those relations. Since the greedy plan is one of the plans considered by DPhyp (it corresponds to a specific join tree), the DPhyp result is at most as expensive as the greedy subtree. Therefore, the replacement can only decrease (or maintain) the cost of the subtree, and thus the total plan cost. ∎

### 18.10 Cardinality Estimation Bounds

**Invariant 6 (Selectivity input validation)**: `addJoinPredicate(rel1, rel2, pred, sel)` requires `0 < sel ≤ 1`.

This is not currently enforced in code. To maintain Invariant 5 (join bound: `card(R ⋈ S) ≤ card(R) × card(S)`), callers must provide selectivity values in the range (0, 1]. An assertion should be added to `addJoinPredicate`:
```java
assert selectivity > 0.0 && selectivity <= 1.0
    : "Join selectivity must be in (0, 1], got " + selectivity;
```
