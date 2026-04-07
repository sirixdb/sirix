# SirixDB Cost-Based Query Optimizer

## Design Document, Code Tour & Educational Guide

---

## Table of Contents

1. [What Is a Cost-Based Optimizer and Why Do We Need One?](#1-what-is-a-cost-based-optimizer-and-why-do-we-need-one)
2. [Architecture Overview](#2-architecture-overview)
3. [The 10-Stage Pipeline](#3-the-10-stage-pipeline)
4. [Stage 1: JQGM Rewrite Rules (Logical Optimization)](#4-stage-1-jqgm-rewrite-rules-logical-optimization)
5. [Stage 2: Cost-Based Analysis](#5-stage-2-cost-based-analysis)
6. [Stage 3: Join Reordering](#6-stage-3-join-reordering)
7. [Stages 4-5: Mesh Population & Selection](#7-stages-4-5-mesh-population--selection)
8. [Stage 6: Index Decomposition (Rules 5-6)](#8-stage-6-index-decomposition-rules-5-6)
9. [Stage 7: Cost-Driven Routing](#9-stage-7-cost-driven-routing)
10. [Stages 8-9: Vectorized Execution](#10-stages-8-9-vectorized-execution)
11. [Stage 10: Index Matching (Physical Rewrite)](#11-stage-10-index-matching-physical-rewrite)
12. [Statistics Infrastructure](#12-statistics-infrastructure)
13. [Adaptive Re-Optimization](#13-adaptive-re-optimization)
14. [Plan Cache](#14-plan-cache)
15. [EXPLAIN Function & QueryPlan API](#15-explain-function--queryplan-api)
16. [Runtime Integration](#16-runtime-integration)
17. [Testing Strategy](#17-testing-strategy)
18. [Glossary](#18-glossary)
19. [References](#19-references)
20. [Code Tour: File Map](#20-code-tour-file-map)

---

## 1. What Is a Cost-Based Optimizer and Why Do We Need One?

### The Problem

Consider this query over a JSON document with 10 million product records:

```xquery
for $product in jn:doc('shop','products')[]
where $product.price > 9990
return $product.name
```

Without an optimizer, the database would:
1. Scan **all 10 million records** sequentially
2. Check each record's `price` field
3. Return the ~10 records that match

With a CAS (Content And Structure) index on `price`, the database could instead:
1. Look up `price > 9990` in the index directly
2. Read only the ~10 matching records

The first approach reads 10 million records. The second reads ~10. But the optimizer must **decide which approach is cheaper**, because indexes aren't always faster — for a query like `where $product.price > 0` that matches *everything*, the index lookup adds overhead without filtering anything out.

### What a Cost-Based Optimizer Does

A cost-based optimizer is a compiler component that:

1. **Considers multiple execution strategies** ("plans") for the same query
2. **Estimates the cost** of each plan using statistics about the data (how many records exist, how values are distributed, what indexes are available)
3. **Picks the cheapest plan**

This is exactly what happens when you use a GPS navigator: it considers multiple routes, estimates travel time for each (using traffic data = statistics), and picks the fastest one. A "rule-based" optimizer would be like always taking highways — usually good, but not when the highway is congested.

### How This Fits Into SirixDB

SirixDB is a **bitemporal database**: it stores every revision of your data immutably. This creates unique optimization challenges:

- **Time-travel queries** (`jn:doc('db','res',5)`) target a specific historical revision. The data distribution at revision 5 may be completely different from revision 50.
- **Immutable revisions** mean statistics collected for revision 5 are **forever valid** — the data can never change. This is a major advantage over traditional databases where statistics go stale.
- **Multiple index types** (CAS, PATH, NAME) serve different query patterns, and the optimizer must pick the right one — or decide that no index helps.

### Scale of the Implementation

- ~8,900 lines of optimizer code
- ~8,300 lines of tests
- 91 files total
- 10 optimization stages
- Based on academic research from TU Kaiserslautern, VLDB, and SIGMOD

---

## 2. Architecture Overview

### The Big Picture

When you execute a query in SirixDB, it goes through three phases:

```
  ┌─────────────────────────────────────────────────────────────┐
  │                     Query Compilation                       │
  │                                                             │
  │   Query String ──► Parser ──► AST ──► Optimizer ──► AST'   │
  │                              (tree)   (10 stages)  (better  │
  │                                                     tree)   │
  └─────────────────────────────────┬───────────────────────────┘
                                    │
  ┌─────────────────────────────────▼───────────────────────────┐
  │                      Translation                            │
  │                                                             │
  │   AST' ──► SirixTranslator ──► Physical Operators           │
  │            (reads annotations)  (TableJoin, IndexExpr, etc) │
  └─────────────────────────────────┬───────────────────────────┘
                                    │
  ┌─────────────────────────────────▼───────────────────────────┐
  │                       Execution                             │
  │                                                             │
  │   Physical Operators ──► Cursor iteration ──► Results       │
  └─────────────────────────────────────────────────────────────┘
```

**Key insight**: The optimizer doesn't produce a separate "plan" data structure. Instead, it **annotates the existing AST** (Abstract Syntax Tree) with properties like "use index #7" or "estimated cardinality: 150". The translator reads these annotations when building physical operators. This keeps the optimizer non-invasive — the AST is the plan.

### What Is an AST?

An AST (Abstract Syntax Tree) is a tree representation of a parsed query. For example:

```xquery
for $x in jn:doc('db','res')[] where $x.price > 50 return $x.name
```

Becomes roughly:

```
FlowrExpr
├── ForBind
│   ├── Variable: $x
│   └── FunctionCall: jn:doc('db','res')
│       └── ArrayAccess: []
├── Selection (where)
│   └── ComparisonExpr: $x.price > 50
└── Return
    └── DerefExpr: $x.name
```

Each node in this tree is a Brackit `AST` object. The optimizer walks this tree, analyzes it, and annotates nodes with optimization decisions stored as key-value properties.

### Entry Points

**`SirixCompileChain`** (`SirixCompileChain.java`) is the entry point. It extends Brackit's `CompileChain` and plugs in:
- `SirixOptimizer` (our 10-stage optimizer) via `getOptimizer()`
- `SirixTranslator` (our custom translator) via `getTranslator()`

```java
// How a query gets compiled and executed:
try (var chain = SirixCompileChain.createWithJsonStore(store);
     var ctx = SirixQueryContext.createWithJsonStore(store)) {
    new Query(chain, "for $x in jn:doc('db','res')[] return $x").serialize(ctx, writer);
}
```

### AST Properties — The Communication Channel

Optimization decisions flow between stages via **AST properties**. These are string-keyed values attached to AST nodes. All 40+ property keys are defined as constants in `CostProperties.java` to prevent typos:

```java
// CostProperties.java — centralized property key constants
public static final String PREFER_INDEX = "costBased.preferIndex";       // Boolean
public static final String INDEX_SCAN_COST = "costBased.indexScanCost";  // Double
public static final String ESTIMATED_CARDINALITY = "costBased.estimatedCardinality"; // Long
// ... 37 more keys
```

This is analogous to how HTTP headers carry metadata alongside the request body — the AST carries the query structure, and properties carry optimization metadata alongside it.

| Category | Example Keys | Set By | Read By |
|----------|-------------|--------|---------|
| Cost hints | `PREFER_INDEX`, `INDEX_SCAN_COST`, `SEQ_SCAN_COST` | CostBasedStage | MeshPopulation, CostDrivenRouting |
| Cardinality | `ESTIMATED_CARDINALITY`, `PATH_CARDINALITY` | CardinalityEstimator | JoinReorder, MeshPopulation |
| Join metadata | `JOIN_COST`, `JOIN_LEFT_CARD`, `JOIN_SWAPPED` | JoinReorderStage | MeshPopulation, MeshSelection |
| Fusion | `JOIN_FUSED`, `FUSED_OPERATOR` | JqgmRewriteStage | CostBasedJoinReorder |
| Decomposition | `DECOMPOSITION_TYPE`, `INTERSECTION_JOIN` | IndexDecompositionStage | SirixPipelineStrategy |
| Mesh | `MESH_CLASS_ID` | MeshPopulationStage | MeshSelectionStage |
| Routing | `INDEX_GATE_CLOSED` | CostDrivenRoutingStage | IndexMatching walkers |

---

## 3. The 10-Stage Pipeline

The optimizer is a **pipeline** of 10 stages. Each stage implements the `Stage` interface with a single method: `AST rewrite(StaticContext sctx, AST ast)`. The stages run sequentially, each receiving the AST from the previous stage and returning a (possibly modified) AST.

**`SirixOptimizer`** (`SirixOptimizer.java`) assembles and runs the pipeline:

```
 Stage  Class                      What It Does (One Sentence)
 ─────  ─────                      ─────────────────────────────
   1    JqgmRewriteStage           Reorganize the query logically (push predicates down, fuse joins)
   2    CostBasedStage             Estimate how much each operation costs and whether indexes help
   3    JoinReorderStage           Find the cheapest order to join multiple tables/collections
   4    MeshPopulationStage        Record all the alternative plans we've considered
   5    MeshSelectionStage         Pick the winner from each set of alternatives
   6    IndexDecompositionStage    Restructure joins to exploit index boundaries
   7    CostDrivenRoutingStage     Tell downstream stages "don't use the index here, it's too expensive"
   8    VectorizedDetectionStage   Detect simple scan-filter patterns that can use SIMD
   9    VectorizedRoutingStage     Replace those patterns with SIMD operators
  10    IndexMatching              Actually rewrite the AST to use specific indexes
```

### Why This Order?

The ordering is deliberate:
- **Logical rewrites first** (Stage 1): Simplify the query before analyzing costs, so the cost model sees a cleaner structure.
- **Cost analysis before join reordering** (Stages 2-3): The join reorderer needs cardinality estimates to compare join orders. Those estimates come from Stage 2.
- **Mesh before decomposition** (Stages 4-6): The Mesh records the alternative plans. Decomposition happens after the best alternative is selected, so it only restructures the chosen plan.
- **Routing before index matching** (Stages 7, 10): The routing stage decides which subtrees should use indexes. Index matching in Stage 10 respects those decisions via the `INDEX_GATE_CLOSED` flag.

### Circuit Breaker

The pipeline has a **50ms timeout** (configurable in `SirixOptimizer.java:29`). If the 10 stages haven't completed within 50ms, the optimizer returns the partially-optimized AST. This prevents pathological queries (e.g., 50-way joins) from blocking execution indefinitely.

For context: PostgreSQL typically optimizes queries in <1ms; CockroachDB budgets ~10ms. Our 50ms budget is generous and only activates for extreme cases.

---

## 4. Stage 1: JQGM Rewrite Rules (Logical Optimization)

### What Are Logical Rewrites?

Logical rewrites transform a query into an **equivalent but more efficient form** without considering physical execution details (indexes, I/O costs, etc.). Think of it like algebraic simplification: `2x + 3x = 5x` is the same math, just simpler.

**`JqgmRewriteStage`** (`JqgmRewriteStage.java`) applies three rewrite "walkers" (tree visitors) based on the JQGM model from Weiner et al.:

### Rule 1: Join Fusion (`JoinFusionWalker.java`, 194 lines)

**Problem**: The parser may produce a chain of binary (two-input) joins: `A JOIN B`, then the result `JOIN C`. But the join reorderer (Stage 3) needs to see all three relations at once to find the optimal order.

**Solution**: Walk the AST looking for adjacent joins that share variables. Annotate them with a shared `JOIN_FUSION_GROUP_ID` so Stage 3 treats them as one n-way join.

**Example**:
```
Before: Join(Join(A, B), C)   -- two separate binary joins
After:  Join(A, B, C)         -- fused: reorderer can consider all 3! = 6 orderings
```

### Rule 2: Select-Join Fusion (`SelectJoinFusionWalker.java`, 96 lines)

**Problem**: A WHERE predicate sitting above a join could be pushed into the join condition, filtering rows earlier and reducing intermediate result sizes.

**Solution**: Identify predicates that reference variables from both sides of a join. Annotate them with `JOIN_PREDICATE_PUSHED=true` so downstream stages know the predicate can be evaluated during the join, not after.

**Analogy**: Instead of inviting 1000 people to a party and then checking IDs at the door, check IDs before they enter the bus (push the filter earlier in the pipeline).

### Rule 3: Select-Access Fusion (`SelectAccessFusionWalker.java`, 211 lines)

**Problem**: A WHERE predicate like `$x.price > 50` is separate from the `$x.price` field access. But for CAS index matching (Stage 10) to work, the predicate information needs to be **on the access node itself**.

**Solution**: When a filter predicate references a field accessed via a deref chain (e.g., `$x.price`), fuse the predicate operator and value into the access node via properties (`FUSED_OPERATOR`, `FUSED_FIELD_NAME`). Stage 10's `JsonCASStep` reads these to determine if a CAS index can serve the predicate.

---

## 5. Stage 2: Cost-Based Analysis

### What Happens Here

This is the **core** of the optimizer. `CostBasedStage` (`CostBasedStage.java`, 477 lines) answers the fundamental question: **for each data access in the query, is it cheaper to scan all records or use an index?**

For each ForBind/LetBind node (which represents a loop over data), it:

1. **Extracts the JSON path** from the `jn:doc('db','res')[]...` expression
2. **Looks up statistics** — how many records match this path? What's the value distribution?
3. **Checks for indexes** — is there a CAS/PATH/NAME index that covers this path?
4. **Estimates selectivity** — what fraction of records does the WHERE predicate filter out?
5. **Calculates costs** — sequential scan cost vs. index scan cost
6. **Annotates the AST** with the decision

### The Cost Model (`JsonCostModel.java`, 229 lines)

The cost model assigns a **numeric cost** to each physical operation. Costs are unitless — they only need to be **comparable** to each other (like comparing GPS route times).

The model captures SirixDB's specific storage architecture:

**Sequential scan** — reading the entire document:
```
cost = pages × IO_COST_PER_PAGE + tuples × CPU_COST_PER_TUPLE
     = (totalNodes / 1024) × 1.0 + totalNodes × 0.01
```
Why is this relatively cheap? SirixDB's keyed trie stores data in document order, so sequential reads benefit from OS prefetching (io_uring). Reading 1000 consecutive pages is much faster than reading 1000 random pages.

**Index scan** — looking up records via a HOT (Height Optimized Trie) index:
```
cost = SETUP + TRAVERSAL_IO + DATA_IO + CPU
     = 3.0 + log16(pathCard) × 0.2 × 4.0 + ceil(pathCard/512) × 4.0 + pathCard × 0.01
```
Why the 3.0 setup cost? Opening an index requires reading the root page and initializing the iterator. Why `log16`? HOT tries have a fanout of ~16 (each internal node has ~16 children), so the tree height is `log16(n)`. The `0.2` factor means only 20% of trie levels cause actual I/O — upper levels are typically cached in the buffer pool.

**When does the index win?** For a 10-million-row document where only 10 rows match:
- Sequential scan: `10000 × 1.0 + 10M × 0.01 = 110,000`
- Index scan: `3.0 + 5.2 × 0.8 + 0.02 × 4.0 + 10 × 0.01 = 7.34`

The index is 15,000x cheaper. But for a query matching 90% of rows:
- Sequential scan: same 110,000
- Index scan: `3.0 + 5.2 × 0.8 + 17578 × 4.0 + 9M × 0.01 = 160,319`

Here the sequential scan wins because the index reads almost as many pages but adds traversal overhead.

### Selectivity Estimation (`SelectivityEstimator.java`, 302 lines)

**Selectivity** is the fraction of rows that pass a predicate. If `price > 50` matches 30% of products, the selectivity is 0.30. This is crucial because it determines how many rows an index scan actually reads.

**Default estimates** (when no histogram exists):
| Predicate | Default Selectivity | Reasoning |
|-----------|-------------------|-----------|
| `x = 42` | 1% (0.01) | Equality is usually very selective |
| `x > 50` | 33% (0.33) | Range typically selects a third |
| `x LIKE '%foo%'` | 25% (0.25) | Pattern match is moderately selective |
| Unknown | 50% (0.5) | Conservative default |

**With histograms** (data-driven estimates):
When a histogram is available for the field, the estimator uses the actual value distribution instead of defaults. For example, if a histogram shows that 90% of prices are between 0-100 and `price > 9990` matches only 0.1% of data, the selectivity is 0.001 — much more accurate than the default 33%.

**Compound predicates** — the tricky part:
- **AND**: `price > 50 AND category = 'books'` — are these independent? Naive multiplication (0.33 × 0.01 = 0.0033) often **underestimates** because predicates may be correlated (expensive products are often in specific categories). We use **SQL Server's exponential backoff**: sort selectivities ascending, then `s[0] × s[1]^(1/2) × s[2]^(1/3) × ...`. This dampens the effect of additional predicates.
- **OR**: `price < 10 OR price > 9990` — uses **inclusion-exclusion**: `P(A∪B) = P(A) + P(B) - P(A)×P(B)`. This prevents overestimation from naive addition.

### Cardinality Estimation (`CardinalityEstimator.java`, 191 lines)

**Cardinality** = the number of rows a pipeline stage produces. The estimator walks the FLWOR expression tree and calculates how many rows each node outputs:

| FLWOR Node | Formula | Intuition |
|------------|---------|-----------|
| ForBind | `inputCard × bindingCard` | Each input row generates `bindingCard` rows from the binding expression |
| LetBind | `inputCard` | Let bindings don't change the number of rows |
| Selection (WHERE) | `inputCard × selectivity` | The predicate filters out rows |
| GroupBy | `distinctCount` or `inputCard / 100` | Grouping reduces rows to the number of distinct groups |
| OrderBy | `inputCard` | Sorting doesn't change the count |
| Join | `leftCard × rightCard × 0.1` | Default assumption: 10% of cross-product rows match |

These estimates feed into join reordering (Stage 3), where the optimizer needs to know how many rows each side of a join produces to pick the cheapest join order.

---

## 6. Stage 3: Join Reordering

### Why Join Order Matters

Consider joining three tables A (1000 rows), B (10 rows), C (100 rows):

- **Order A-B-C**: Join A×B = 1000×10 comparisons, then result×C = 100×100 comparisons. Total: 20,000.
- **Order B-A-C**: Join B×A = 10×1000 comparisons, then result×C = 100×100 comparisons. Total: 20,000.
- **Order B-C-A**: Join B×C = 10×100 comparisons, then result×A = 10×1000 comparisons. Total: 11,000.

The third order is almost 2x cheaper! With 10 tables, there are 10! = 3.6 million possible orderings. Brute-force enumeration is impossible for large joins.

### The DPhyp Algorithm (`AdaptiveJoinOrderOptimizer.java`, 300 lines)

We use **DPhyp** (Dynamic Programming via Hypergraph Partitioning), the same algorithm used in HyPer and Umbra — two of the fastest analytical databases in existence.

**How it works** (simplified):

1. Represent the join as a **graph**: each table is a node, each join condition is an edge.
2. Use **dynamic programming** to find the cheapest way to join subsets of tables.
3. The key insight: you don't need to enumerate all orderings. DPhyp only considers **connected subgraph complement pairs** — subsets where removing one subset leaves the other connected. This dramatically prunes the search space.
4. Results are memoized in a `DpTable` — a hash table mapping **bitmasks** (which tables are included) to the best plan for that subset.

**Complexity**: O(n·3^n) for n relations. This is fast for up to ~20 relations. For the example above (3 tables, bitmasks 001, 010, 100):
- Base plans: {A}=001, {B}=010, {C}=100
- Try pairs: {A}+{B}=011 (cost?), {A}+{C}=101 (cost?), {B}+{C}=110 (cost?)
- Try triples: {A,B}+{C}=111 (cost?), {A,C}+{B}=111 (cost?), {B,C}+{A}=111 (cost?)
- Pick cheapest 111 plan.

### GOO Fallback for Large Joins

For queries with >20 relations (e.g., auto-generated BI queries), DPhyp's 3^n becomes impractical. We fall back to **GOO** (Greedy Operator Ordering):

1. Start with one plan per relation.
2. Find the cheapest connected pair to merge.
3. Replace the pair with a single joined plan.
4. Repeat until one plan remains.

GOO is O(n^3) — always fast, but not guaranteed optimal. In practice, the difference from DPhyp is small for most query shapes.

### The DpTable (`DpTable.java`, 127 lines)

The DP memoization table uses **open-addressing with Fibonacci hashing** — a hash table where collisions are resolved by probing subsequent slots. Fibonacci hashing (`key × 0x9E3779B97F4A7C15L >>> shift`) provides excellent distribution for bitmask keys, minimizing cache misses.

---

## 7. Stages 4-5: Mesh Population & Selection

### What Is the Mesh?

The **Mesh** (named after Graefe & DeWitt's Exodus optimizer framework) is a data structure that records **all the alternative plans** the optimizer has considered. Think of it as a "decision log" — for each point in the query where a choice exists (index scan vs. sequential scan, join order A vs. B), the Mesh records both alternatives with their costs.

This serves two purposes:
1. **Plan selection**: Pick the cheapest alternative at each decision point.
2. **EXPLAIN output**: Show the user what alternatives were considered (via `sdb:explain($query, 'candidates')`).

### How It's Organized

The Mesh groups alternatives into **equivalence classes**. An equivalence class contains plans that produce the same result but differ in execution strategy:

```
EquivalenceClass #0: "How to access products"
  ├── Alternative A: Sequential scan      (cost: 110,000)
  └── Alternative B: CAS index on price   (cost: 7.34)     ← winner

EquivalenceClass #1: "How to join products × orders"
  ├── Alternative A: Products first, then orders  (cost: 5,000)
  └── Alternative B: Orders first, then products  (cost: 3,200)  ← winner
```

### Stage 4: MeshPopulationStage (`MeshPopulationStage.java`, 179 lines)

Walks the AST and creates equivalence classes:
- For each ForBind with `PREFER_INDEX` annotations: creates class with seq scan + index scan alternatives
- For each Join with cost annotations: creates class with current order + swapped order alternatives
- Tags each AST node with its `MESH_CLASS_ID` for Stage 5 to find

### Stage 5: MeshSelectionStage (`MeshSelectionStage.java`, 145 lines)

Walks the AST **bottom-up** (children before parents) and applies the best plan from each equivalence class:

**Why bottom-up?** Because a parent's cost depends on its children's costs. If child node "access products" switches from sequential scan (cost 110,000) to index scan (cost 7.34), the parent join's total cost changes dramatically. By resolving children first, parent decisions see accurate child costs.

For joins, if the best plan has a different ordering than the current AST, Stage 5 **physically swaps the join children** — this is one of the few places where the optimizer actually restructures the AST rather than just annotating it.

---

## 8. Stage 6: Index Decomposition (Rules 5-6)

### The Problem

Sometimes a join has one side that can use an index and another side that can't. Or both sides can use *different* indexes. These situations require restructuring the join to exploit the index boundaries.

### Rule 5: Indexed Side Drives the Join

**Scenario**: `products JOIN reviews` where `products` has a CAS index on `price` but `reviews` has no useful index.

**Optimization**: Swap the join inputs so the indexed side (`products`) is the **driving side** (child 0). In a nested-loop or hash join, the driving side is scanned first, and its results probe the other side. When the driving side is small (thanks to the index filter), fewer probes are needed.

**Implementation** (`IndexDecompositionStage.applyRule5()`):
```java
if (!leftHasIndex) {
    // Index is on the right — swap so indexed side drives
    final AST leftInput = join.getChild(0);
    final AST rightInput = join.getChild(1);
    join.replaceChild(0, rightInput);
    join.replaceChild(1, leftInput);
}
```

### Rule 6: Intersection Join

**Scenario**: `products JOIN products` (self-join) where both sides of the join use *different* indexes — e.g., one side has a CAS index on `price` and the other has a PATH index on `category`.

**Optimization**: Both sides become index scans, and the join becomes an **intersection** of two index result sets. Both results are already filtered by their respective indexes, so the join only processes the intersection.

**Implementation** (`IndexDecompositionStage.applyRule6()`):
```java
join.setProperty(CostProperties.INTERSECTION_JOIN, true);
propagatePreferIndex(join.getChild(0), MAX_SEARCH_DEPTH);
propagatePreferIndex(join.getChild(1), MAX_SEARCH_DEPTH);
```

### Two-Phase Design

The stage uses a two-phase approach:
1. **Phase 1** (Walker): Annotates joins with decomposition metadata (`DECOMPOSITION_TYPE`, `DECOMPOSITION_INDEX_ID`, etc.). This is a read-only analysis pass.
2. **Phase 2** (Restructuring): Reads the annotations and mutates the AST (swaps children, sets flags). This is a separate post-order walk.

Why two phases? Because the Walker base class traverses the tree top-down and may re-visit mutated children unpredictably. Separating analysis from mutation avoids these issues.

---

## 9. Stage 7: Cost-Driven Routing

### The Gate Metaphor

**`CostDrivenRoutingStage`** (`CostDrivenRoutingStage.java`, 72 lines) translates the cost model's recommendations into **gate signals** that control index matching in Stage 10.

Think of it like traffic lights at an intersection:
- **Green light** (`PREFER_INDEX=true`): "Go ahead and use the index."
- **Red light** (`INDEX_GATE_CLOSED=true`): "Don't use the index — sequential scan is cheaper."

When Stage 2 annotates a ForBind with `PREFER_INDEX=false`, Stage 7 propagates `INDEX_GATE_CLOSED=true` to **all descendant nodes**. When Stage 10's index matching walkers visit those nodes, they check the gate and skip the rewrite.

Without this stage, the index matching walkers would blindly rewrite every eligible node to use an index, even when the cost model determined it would be slower.

---

## 10. Stages 8-9: Vectorized Execution

### What Is Vectorized Execution?

Traditional query execution processes one row at a time: read a row, check the predicate, output if it matches, repeat. **Vectorized execution** processes rows in **batches** (e.g., 1024 at a time) using CPU SIMD instructions that operate on multiple data elements simultaneously.

For simple scan-filter-project queries (no joins, no grouping), vectorized execution can be 5-10x faster because:
- Fewer function calls (one per batch instead of one per row)
- Better CPU cache utilization (batch fits in L1/L2)
- SIMD instructions process 4-16 comparisons in a single CPU cycle

### Stage 8: VectorizedDetectionStage

Identifies scan-filter-project pipelines eligible for SIMD: a ForBind over `jn:doc()` with simple equality/range predicates and field projections. Complex queries (joins, grouping, subqueries) aren't eligible.

### Stage 9: VectorizedRoutingStage (`VectorizedRoutingStage.java`, 458 lines)

Replaces eligible pipeline subtrees with `VectorizedPipelineExpr` AST nodes. These nodes bypass the normal tuple-at-a-time execution and use columnar batch processing with FSST-aware string comparison (FSST = Fast Static Symbol Table, a string compression scheme).

---

## 11. Stage 10: Index Matching (Physical Rewrite)

### What Happens Here

This is where the optimizer **physically rewrites the AST** to use specific indexes. All previous stages only annotated the AST with metadata; Stage 10 actually replaces AST subtrees with `IndexExpr` nodes.

**`IndexMatching`** (inner class in `SirixOptimizer.java`) runs three walkers in priority order:

1. **`JsonCASStep`** (327 lines): Matches CAS (Content And Structure) indexes. These indexes store `(value, path)` pairs in a B+-tree, enabling efficient value-based lookups. Example: A CAS index on `/[]/price` of type `xs:integer` can serve `$x.price > 50`.

2. **`JsonPathStep`** (75 lines): Matches PATH indexes. These indexes map paths to node keys, enabling efficient path-based access without scanning the entire document. Example: A PATH index on `/[]/item/name` can serve `$x.item.name` field access.

3. **`JsonObjectKeyNameStep`** (76 lines): Matches NAME indexes on object keys. These enable efficient `$x.fieldName` access when many objects have different key sets.

### The Rewrite

When a walker finds a match, it:
1. Creates an `IndexExpr` AST node with the index definition, database/resource metadata, and filter parameters
2. Replaces the original AST subtree with the `IndexExpr` node
3. At runtime, `IndexExpr` opens the index directly instead of scanning the document

### The Cost Gate Check

Before applying any rewrite, every walker checks:
```java
if (CostProperties.isIndexGateClosed(astNode)) {
    return astNode;  // Skip — cost model says sequential scan is cheaper
}
```

This is how Stages 2-7 communicate their cost decisions to Stage 10.

---

## 12. Statistics Infrastructure

### Why Statistics Matter

Without statistics, the optimizer uses **default estimates** (1% for equality, 33% for range). These are often wildly wrong:
- A status field with 90% of rows having `status='active'` → equality selectivity is 90%, not 1%
- A price field where 99% of values are between 0-100 → `price > 99.5` matches 0.5%, not 33%

Bad estimates lead to bad plans. Statistics fix this.

### Histograms (`Histogram.java`, 504 lines)

A **histogram** summarizes the distribution of values in a field. SirixDB supports two types:

**Equi-width histogram** (default): Divides the value range into N buckets of equal width and counts how many values fall in each bucket. Fast O(1) lookup but can waste resolution on sparse regions.

```
Example: 1000 prices in [0, 100], 10 buckets
Bucket [0,10): 450 values  ← most products are cheap
Bucket [10,20): 200 values
Bucket [20,30): 100 values
...
Bucket [90,100): 5 values  ← few expensive products
```

**Equi-depth histogram**: Each bucket has approximately the same number of values but variable width. Better for skewed data because the dense region gets narrow buckets (more resolution) and the sparse region gets wide buckets.

```
Same data, equi-depth with 4 buckets:
Bucket [0, 3.2): 250 values    ← narrow: dense region, high resolution
Bucket [3.2, 8.5): 250 values  ← narrow
Bucket [8.5, 35): 250 values   ← wider
Bucket [35, 100): 250 values   ← very wide: sparse region
```

**Most-Common-Values (MCV)**: The top-K (default 10) most frequent values are tracked separately with exact frequencies. This handles the common case where a few values dominate:

```
MCVs for status field:
  'active': 9000/10000 = 90%
  'inactive': 800/10000 = 8%
  'pending': 150/10000 = 1.5%

For query "WHERE status = 'active'":
  Without MCV: selectivity = 1/NDV = 1/3 = 33%  (WRONG!)
  With MCV: selectivity = 90%  (CORRECT!)
```

MCVs are subtracted from bucket counts during histogram construction, so the bucket counts only represent the non-MCV distribution. This prevents double-counting when estimating range queries.

### StatisticsCatalog (`StatisticsCatalog.java`, 188 lines)

A singleton LRU cache that maps `(database, resource, field, revision)` to histograms.

**Revision-awareness** — unique to SirixDB's bitemporal architecture:
- **Historical revisions** (revision > 0): The data at revision 5 is immutable — it can never change. So a histogram collected for revision 5 is **forever valid**. No TTL, no invalidation needed.
- **Latest revision** (revision = -1): The "current" data changes with writes. These histograms have a 1-hour TTL and are invalidated after commits.

This is a significant optimization: in a database with 1000 revisions, only the latest revision's histograms ever go stale.

### Automatic Collection

When the optimizer encounters a field with no histogram in the catalog, it records the field for **deferred collection**. After optimization completes, `SirixOptimizer` exposes `collectPendingHistograms()` which callers can invoke to collect histograms asynchronously. The next query against the same field will benefit from the collected histogram.

The collection itself is done by `HistogramCollector` (`HistogramCollector.java`, 300 lines), which:
1. Opens a read-only transaction
2. Walks the document using stride-based sampling (to avoid reading every single value)
3. Builds a histogram from the sampled values
4. Registers it in the `StatisticsCatalog`

This is analogous to PostgreSQL's `ANALYZE` command, but triggered automatically on cache miss.

### Post-Commit Invalidation

After a write transaction commits, `SirixQueryContext.invalidateStatisticsForResource()` fires:
- Invalidates the latest-revision histograms for the modified resource
- Signals `PlanCache.signalIndexSchemaChange()` to invalidate all cached plans

This ensures that the next query after a data modification gets fresh statistics instead of stale ones from before the write.

---

## 13. Adaptive Re-Optimization

### The Problem

Even with good statistics, cardinality estimates can be wrong. The optimizer might estimate that `price > 9990` matches 10 rows, but at runtime it matches 10,000. If this happens repeatedly, the cached plan is suboptimal.

### The Solution: CardinalityTracker (`CardinalityTracker.java`, 113 lines)

After query execution, callers can report the actual result count. The `CardinalityTracker` compares it to the optimizer's estimate and detects **persistent drift**:

- **Threshold**: 10x mismatch between estimated and actual cardinality
- **Damping**: Must happen 3 consecutive times before acting (a single outlier doesn't trigger invalidation)
- **Action**: Surgical plan cache invalidation — only the specific query's plan is evicted

**Why damping?** Imagine a query that usually returns 100 rows but occasionally returns 10,000 during a batch import. Without damping, the first anomalous execution would evict a good plan. With 3-consecutive damping, transient spikes are ignored.

```java
// Example: CardinalityTracker in action
tracker.record("query@v0", 100, 95, planCache);    // ratio 1.05 — accurate, resets counter
tracker.record("query@v0", 100, 5000, planCache);   // ratio 50 — mismatch #1
tracker.record("query@v0", 100, 4800, planCache);   // ratio 48 — mismatch #2
tracker.record("query@v0", 100, 5200, planCache);   // ratio 52 — mismatch #3 → INVALIDATE!
```

---

## 14. Plan Cache

### Why Cache Plans?

Optimization is relatively fast (typically <5ms), but some applications execute the same query thousands of times per second (dashboards, monitoring, APIs). Caching the optimized AST eliminates redundant optimization work.

**`PlanCache`** (`PlanCache.java`, 162 lines) is a simple LRU cache:

- **Key**: `queryText + "@v" + indexSchemaVersion` — the version suffix ensures plans are invalidated when indexes are created or dropped
- **Capacity**: 128 plans (LRU eviction)
- **Deep copy safety**: Both `get()` and `put()` use `AST.copyTree()` to prevent downstream code from accidentally mutating cached plans

### Schema Version

The `indexSchemaVersion` is a global `AtomicLong` that's incremented whenever indexes change. This makes the cache key version-aware:

```
Before index creation:  "for $x in ..." + "@v0"  → plan without index
After index creation:   "for $x in ..." + "@v1"  → cache miss → re-optimize with index
```

---

## 15. EXPLAIN Function & QueryPlan API

### For Users: sdb:explain()

```xquery
(: Show the optimized plan :)
sdb:explain('for $x in jn:doc("db","res")[] where $x.price > 50 return $x')

(: Show both parsed and optimized plans :)
sdb:explain('for $x in jn:doc("db","res")[] where $x.price > 50 return $x', true())

(: Show all candidate plans from the Mesh :)
sdb:explain('for $x in jn:doc("db","res")[] where $x.price > 50 return $x', 'candidates')
```

### For Developers: QueryPlan API (`QueryPlan.java`, 233 lines)

```java
QueryPlan plan = QueryPlan.explain(query, jsonStore, xmlStore);

// Inspection methods:
plan.usesIndex()                   // Was an IndexExpr actually created? (ground truth)
plan.prefersIndex()                // Did the cost model recommend an index? (recommendation)
plan.indexType()                   // "CAS", "PATH", "NAME", or null
plan.estimatedCardinality()        // How many rows does the optimizer expect?
plan.isIntersectionJoin()          // Was Rule 6 applied?
plan.isDecompositionRestructured() // Were Rules 5 or 6 applied?
plan.isJoinReordered()             // Was DPhyp/GOO join reordering applied?
plan.hasClosedGate()               // Did the cost model close the index gate?
plan.vectorizedRoute()             // "columnar" if SIMD routing applied

// Serialization:
plan.toJSON()                      // Human-readable optimized plan
plan.toVerboseJSON()               // Both parsed and optimized
plan.toCandidatesJSON()            // Plan + all Mesh alternatives
```

**Note**: `usesIndex()` and `prefersIndex()` can differ! The cost model might prefer an index (`PREFER_INDEX=true`), but no matching CAS/PATH/NAME index exists for the specific predicate pattern, so `IndexMatching` (Stage 10) doesn't create an `IndexExpr`. `prefersIndex()` reflects the recommendation; `usesIndex()` reflects reality.

---

## 16. Runtime Integration

### SirixPipelineStrategy (`SirixPipelineStrategy.java`, 42 lines)

Extends Brackit's join compilation to support optimizer annotations:

When `INTERSECTION_JOIN=true` is detected on a Join node, forces `skipSort=true` on the hash join operator. This skips the O(n log n) post-probe sort and deduplication — correct for intersection joins because index results are already unique by nodeKey.

### Physical Operators

**`IndexExpr`** (`IndexExpr.java`, 406 lines): Replaces sequential scans with direct index lookups. Dispatches on index type (PATH, CAS, NAME), opens the appropriate index via `IndexController`, and materializes results into an `ItemSequence`.

**`VectorizedPipelineExpr`** (`VectorizedPipelineExpr.java`, 343 lines): Replaces simple scan-filter-project pipelines with batch-oriented columnar execution using SIMD instructions.

---

## 17. Testing Strategy

### Four-Tier Approach

**33 test files, ~8,300 lines** organized by scope:

| Tier | What It Tests | Example | Count |
|------|--------------|---------|-------|
| 1. Unit | Individual components in isolation | `HistogramTest`: Does equi-depth partitioning produce equal-count buckets? | 8 files |
| 2. Stage | One optimizer stage with mock ASTs | `MeshSelectionStageTest`: Does bottom-up selection swap join children correctly? | 6 files |
| 3. Walker | Individual rewrite walkers | `JoinFusionWalkerTest`: Does Rule 1 detect adjacent fusable joins? | 5 files |
| 4. E2E | Full pipeline with real databases | `OptimizerBehavioralE2ETest`: Store 10K rows, create CAS index, verify plan uses it AND results are correct | 4 files |

### The E2E Tests Verify TWO Things

Each E2E test verifies both the **plan** and the **results**:

```java
// 1. Verify the optimizer makes the right decision
QueryPlan plan = QueryPlan.explain(query, store, null);
assertTrue(plan.usesIndex(), "CAS index should be used for 10K rows");
assertEquals("CAS", plan.indexType());

// 2. Verify the query returns correct results
assertEquals("9991 9992 9993 9994 9995 9996 9997 9998 9999 10000",
    executeQuery("... where $x.price gt 9990 order by $x.price return $x.price"));
```

This catches a class of bugs where the optimizer makes a correct decision but the execution produces wrong results (or vice versa).

---

## 18. Glossary

| Term | Definition |
|------|-----------|
| **AST** | Abstract Syntax Tree — tree representation of a parsed query |
| **Cardinality** | Number of rows a query operation produces |
| **CAS Index** | Content And Structure — B+-tree index on `(value, path)` pairs |
| **Circuit Breaker** | Timeout that aborts optimization if it takes too long |
| **DPhyp** | Dynamic Programming via Hypergraph Partitioning — optimal join ordering algorithm |
| **Equi-depth** | Histogram where each bucket has the same number of values |
| **Equi-width** | Histogram where each bucket spans the same value range |
| **FLWOR** | For-Let-Where-OrderBy-Return — the XQuery/JSONiq loop construct |
| **GOO** | Greedy Operator Ordering — fast fallback for large join graphs |
| **HOT** | Height Optimized Trie — SirixDB's index structure with SIMD search |
| **Index Gate** | Flag that prevents index matching when the cost model prefers sequential scan |
| **JQGM** | JSON Query Graph Model — adaptation of Weiner's XQGM for JSON |
| **MCV** | Most-Common Values — exact frequencies for the K most frequent values |
| **Mesh** | Search space structure grouping equivalent plans into equivalence classes |
| **NDV** | Number of Distinct Values — used for equality selectivity estimation |
| **PATH Index** | Index mapping JSON paths to node keys |
| **PathSummary** | SirixDB structure summarizing all paths in a document with cardinalities |
| **Plan Cache** | LRU cache storing optimized ASTs to avoid redundant optimization |
| **Selectivity** | Fraction of rows passing a predicate (0.0 = matches nothing, 1.0 = matches everything) |
| **SIMD** | Single Instruction Multiple Data — CPU instructions processing multiple values at once |

---

## 19. References

1. **Weiner et al.** "Cost-Based Optimization of Integration Flows" (TU Kaiserslautern, CEUR-WS 2008) — XQGM architecture, 6 rewrite rules, Mesh search space
2. **Moerkotte & Neumann** "Analysis of Two Existing and One New Dynamic Programming Algorithm for the Generation of Optimal Bushy Join Trees without Cross Products" (VLDB 2006) — DPhyp algorithm
3. **Neumann & Radke** "Adaptive Optimization of Very Large Join Queries" (SIGMOD 2018) — GOO fallback for large join graphs
4. **Graefe & DeWitt** "The EXODUS Optimizer Generator" (SIGMOD 1987) — Mesh/equivalence class search space
5. **PostgreSQL** `src/backend/optimizer/path/costsize.c` — Selectivity defaults, exponential backoff for AND, equi-depth histograms, MCV tracking

---

## 20. Code Tour: File Map

```
bundles/sirix-query/src/main/java/io/sirix/query/
├── SirixCompileChain.java              ← Entry point: creates optimizer + translator
├── SirixQueryContext.java              ← Post-commit statistics invalidation
├── compiler/
│   ├── XQExt.java                      ← Extension AST node types (IndexExpr, VectorizedPipelineExpr)
│   ├── translator/
│   │   ├── SirixTranslator.java        ← Converts optimized AST → physical operators
│   │   └── SirixPipelineStrategy.java  ← Intersection join: forces hash mode on TableJoin
│   ├── expression/
│   │   ├── IndexExpr.java              ← Physical operator: reads from CAS/PATH/NAME index
│   │   └── VectorizedPipelineExpr.java ← Physical operator: SIMD batch scan-filter-project
│   └── optimizer/
│       ├── SirixOptimizer.java         ← Orchestrates 10-stage pipeline with 50ms circuit breaker
│       ├── PlanCache.java              ← LRU cache: queryText+schemaVersion → optimized AST
│       ├── CardinalityTracker.java     ← Detects estimate-vs-actual drift, invalidates stale plans
│       │
│       │  ── Stages ──
│       ├── JqgmRewriteStage.java       ← Stage 1: Logical rewrites (Rules 1-3)
│       ├── CostBasedStage.java         ← Stage 2: Cost analysis + histogram auto-collection
│       ├── JoinReorderStage.java       ← Stage 3: DPhyp/GOO join ordering
│       ├── MeshPopulationStage.java    ← Stage 4: Record plan alternatives in Mesh
│       ├── MeshSelectionStage.java     ← Stage 5: Pick best plan per equivalence class
│       ├── IndexDecompositionStage.java ← Stage 6: Rules 5-6 (join restructuring at index boundaries)
│       ├── CostDrivenRoutingStage.java ← Stage 7: Propagate INDEX_GATE_CLOSED flags
│       ├── VectorizedDetectionStage.java ← Stage 8: Detect SIMD-eligible pipelines
│       ├── VectorizedRoutingStage.java ← Stage 9: Replace with VectorizedPipelineExpr
│       │
│       │  ── Join Ordering ──
│       ├── join/
│       │   ├── AdaptiveJoinOrderOptimizer.java ← DPhyp (≤20 rels) + GOO (>20 rels)
│       │   ├── JoinGraph.java          ← Relations + join predicates as a graph
│       │   ├── JoinPlan.java           ← Node in the join plan tree
│       │   ├── JoinEdge.java           ← Edge connecting two relations
│       │   └── DpTable.java            ← Bitmask → best plan (Fibonacci hashing)
│       │
│       │  ── Search Space ──
│       ├── mesh/
│       │   ├── Mesh.java               ← Groups equivalent plans into classes
│       │   ├── EquivalenceClass.java   ← Set of alternative plans with best-cost tracking
│       │   └── PlanAlternative.java    ← Single (plan, cost) pair
│       │
│       │  ── Statistics & Cost Model ──
│       ├── stats/
│       │   ├── CostProperties.java     ← All 40+ AST property key constants
│       │   ├── JsonCostModel.java      ← Sequential scan vs. index scan cost formulas
│       │   ├── SelectivityEstimator.java ← Predicate selectivity (defaults + histogram + MCV)
│       │   ├── CardinalityEstimator.java ← FLWOR pipeline cardinality propagation
│       │   ├── Histogram.java          ← Equi-width + equi-depth + MCV tracking
│       │   ├── HistogramCollector.java  ← Samples data and builds histograms (like ANALYZE)
│       │   ├── StatisticsCatalog.java  ← Revision-aware histogram LRU cache
│       │   ├── SirixStatisticsProvider.java ← PathSummary-backed cardinality + index lookup
│       │   ├── StatisticsProvider.java ← Interface for statistics providers
│       │   ├── IndexInfo.java          ← Metadata about available indexes
│       │   └── BaseProfile.java        ← Physical statistics profile per JSON path
│       │
│       │  ── Rewrite Walkers ──
│       └── walker/
│           └── json/
│               ├── JoinFusionWalker.java         ← Rule 1: Fuse adjacent binary joins
│               ├── SelectJoinFusionWalker.java    ← Rule 2: Push predicates into joins
│               ├── SelectAccessFusionWalker.java  ← Rule 3: Push predicates into access ops
│               ├── JoinCommutativityWalker.java   ← Rule 4: Swap join sides by cost
│               ├── JoinDecompositionWalker.java   ← Rules 5-6: Annotate index decomposition
│               ├── CostBasedJoinReorder.java      ← Extract join groups for DPhyp
│               ├── AbstractJsonPathWalker.java    ← Base class for index matching
│               ├── JsonCASStep.java               ← Match CAS indexes to predicates
│               ├── JsonPathStep.java              ← Match PATH indexes to field access
│               └── JsonObjectKeyNameStep.java     ← Match NAME indexes to object keys
│
└── function/sdb/explain/
    ├── Explain.java                    ← sdb:explain() XQuery function
    ├── QueryPlan.java                  ← Programmatic plan inspection API
    └── QueryPlanSerializer.java        ← Converts AST → human-readable JSON
```
