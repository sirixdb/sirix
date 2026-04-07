# SirixDB Cost-Based Query Optimizer: Design Document & Code Tour

## Table of Contents

1. [Introduction](#1-introduction)
2. [Architecture Overview](#2-architecture-overview)
3. [The 10-Stage Pipeline](#3-the-10-stage-pipeline)
4. [Stage 1: JQGM Rewrite Rules](#4-stage-1-jqgm-rewrite-rules)
5. [Stage 2: Cost-Based Analysis](#5-stage-2-cost-based-analysis)
6. [Stage 3: Join Reordering](#6-stage-3-join-reordering)
7. [Stages 4-5: Mesh Population & Selection](#7-stages-4-5-mesh-population--selection)
8. [Stage 6: Index Decomposition](#8-stage-6-index-decomposition)
9. [Stage 7: Cost-Driven Routing](#9-stage-7-cost-driven-routing)
10. [Stages 8-9: Vectorized Execution](#10-stages-8-9-vectorized-execution)
11. [Stage 10: Index Matching](#11-stage-10-index-matching)
12. [Statistics Infrastructure](#12-statistics-infrastructure)
13. [Adaptive Re-Optimization](#13-adaptive-re-optimization)
14. [Plan Cache](#14-plan-cache)
15. [EXPLAIN Function & QueryPlan API](#15-explain-function--queryplan-api)
16. [Runtime Integration](#16-runtime-integration)
17. [Testing Strategy](#17-testing-strategy)
18. [References](#18-references)

---

## 1. Introduction

SirixDB's cost-based query optimizer transforms JSONiq/XQuery queries into efficient execution plans by reasoning about data distribution, index availability, and operator costs. It is based on the **JQGM (JSON Query Graph Model)** from Weiner et al. (TU Kaiserslautern, CEUR-WS 2008), adapted for JSON paths and SirixDB's unique bitemporal storage architecture.

**Key design decisions:**
- **Property-based AST annotation**: Optimization decisions are recorded as properties on Brackit AST nodes, not as separate plan trees. This avoids maintaining two representations and makes the optimizer non-invasive.
- **Stage-based pipeline**: 10 independent stages run sequentially, each reading/writing AST properties. Stages can be disabled, reordered, or extended without changing others.
- **Revision-aware statistics**: Since SirixDB stores every revision immutably, histogram statistics for historical revisions never go stale. Only the latest revision's statistics require TTL-based expiry.
- **Circuit breaker**: A 50ms timeout prevents optimization from blocking query execution.

**Scale**: ~8,900 lines of optimizer code, ~8,300 lines of tests, across 91 files.

---

## 2. Architecture Overview

```
                          Query String
                               |
                        SirixCompileChain
                               |
                    +----------+-----------+
                    |    Brackit Parser     |
                    +----------+-----------+
                               |
                          Parsed AST
                               |
              +----------------+----------------+
              |        SirixOptimizer           |
              |   (10-stage pipeline, 50ms CB) |
              +----------------+----------------+
                               |
                       Optimized AST
                    (annotated with costs,
                     index decisions, etc.)
                               |
              +----------------+----------------+
              |     SirixTranslator             |
              |  + SirixPipelineStrategy        |
              +----------------+----------------+
                               |
                    Physical Operators
                 (TableJoin, IndexExpr,
                  VectorizedPipelineExpr)
                               |
                         Execution
```

### Entry Point

**`SirixCompileChain`** (`SirixCompileChain.java:38`) orchestrates parsing, optimization, and translation. It creates a `SirixOptimizer` via `getOptimizer()` (line 147) and a `SirixTranslator` via `getTranslator()` (line 136).

### AST Properties

All optimization metadata is stored as string-keyed properties on Brackit `AST` nodes via `node.setProperty(key, value)`. The 40+ property keys are centralized in `CostProperties.java` to prevent typos and enable refactoring:

| Category | Example Keys | Set By |
|----------|-------------|--------|
| Cost hints | `PREFER_INDEX`, `INDEX_SCAN_COST`, `SEQ_SCAN_COST` | CostBasedStage |
| Cardinality | `ESTIMATED_CARDINALITY`, `PATH_CARDINALITY` | CardinalityEstimator, CostBasedStage |
| Join metadata | `JOIN_COST`, `JOIN_LEFT_CARD`, `JOIN_SWAPPED` | JoinReorderStage |
| Fusion | `JOIN_FUSED`, `FUSED_OPERATOR`, `FUSED_FIELD_NAME` | JqgmRewriteStage |
| Decomposition | `DECOMPOSITION_TYPE`, `INTERSECTION_JOIN` | IndexDecompositionStage |
| Mesh | `MESH_CLASS_ID` | MeshPopulationStage |
| Routing | `INDEX_GATE_CLOSED` | CostDrivenRoutingStage |

---

## 3. The 10-Stage Pipeline

**`SirixOptimizer`** (`SirixOptimizer.java:41-71`) assembles the pipeline:

```
 Stage  Class                      Purpose
 ─────  ─────                      ───────
   1    JqgmRewriteStage           Logical rewrites: Rules 1-3 (predicate pushdown, join fusion)
   2    CostBasedStage             Cost analysis: annotate PREFER_INDEX + cardinality estimates
   3    JoinReorderStage           DPhyp/GOO join ordering using cardinality estimates
   4    MeshPopulationStage        Build search space with plan alternatives
   5    MeshSelectionStage         Select best plan per equivalence class (bottom-up)
   6    IndexDecompositionStage    Rules 5-6: restructure joins at index boundaries
   7    CostDrivenRoutingStage     Propagate PREFER_INDEX → INDEX_GATE_CLOSED
   8    VectorizedDetectionStage   Detect SIMD-eligible scan-filter-project pipelines
   9    VectorizedRoutingStage     Route to columnar/SIMD execution
  10    IndexMatching              Final index rewrite (CAS/PATH/NAME)
```

The pipeline runs inline in `optimize()` (line 94-108) with a circuit breaker: if total time exceeds 50ms, the partially-optimized AST is returned.

---

## 4. Stage 1: JQGM Rewrite Rules

**`JqgmRewriteStage`** (`JqgmRewriteStage.java:38`) applies three logical rewrite walkers in sequence:

### Rule 1: Join Fusion (`JoinFusionWalker.java`)

Identifies chains of binary joins that can be fused into n-way joins. Annotates join nodes with `JOIN_FUSED=true` and a shared `JOIN_FUSION_GROUP_ID`. Downstream stages use this to detect fusable join groups.

**Example**: `A JOIN B JOIN C` where both joins share a common variable are annotated as a single fusion group, enabling the join reorderer to consider all three relations simultaneously.

### Rule 2: Select-Join Fusion (`SelectJoinFusionWalker.java`)

Identifies predicates (Selection nodes) that can be pushed into joins. Annotates pushable predicates with `JOIN_PREDICATE_PUSHED=true`. Does not physically move the predicate — that's left to downstream stages.

### Rule 3: Select-Access Fusion (`SelectAccessFusionWalker.java`)

Pushes WHERE predicates into access operators (ObjectAccess/ArrayAccess). When a filter predicate references a field accessed via a deref chain, the walker fuses the predicate info into the access node via `FUSED_OPERATOR`, `FUSED_FIELD_NAME`, etc. This enables CAS index matching in Stage 10.

---

## 5. Stage 2: Cost-Based Analysis

**`CostBasedStage`** (`CostBasedStage.java:43`) is the heaviest stage. For each ForBind/LetBind node in the AST, it:

1. **Extracts the JSON path** from the deref chain rooted at `jn:doc()` (line 182-185)
2. **Looks up histogram** from `StatisticsCatalog` (revision-aware, line 197-205)
3. **Queries PathSummary** for path cardinality and total node count (line 211-214)
4. **Checks for available indexes** via `SirixStatisticsProvider.getIndexInfo()` (line 226)
5. **Estimates predicate selectivity** using `SelectivityEstimator` (line 236)
6. **Compares costs**: sequential scan vs. index scan via `JsonCostModel` (line 239-243)
7. **Annotates the AST** with `PREFER_INDEX`, `INDEX_ID`, `INDEX_TYPE`, costs (line 245-256)
8. **Propagates cardinalities** through the entire FLWOR pipeline via `CardinalityEstimator` (line 113)

### Cost Model (`JsonCostModel.java`)

Models SirixDB's storage architecture:

**Sequential scan** (keyed trie with io_uring prefetch):
```
cost = pages × 1.0 + tuples × 0.01
     = (totalNodeCount / 1024) × SEQ_IO_PER_PAGE + totalNodeCount × CPU_PER_TUPLE
```

**Index scan** (HOT trie with SIMD partial-key search):
```
cost = 3.0 + trieDepth × 0.2 × 4.0 + leafPages × 4.0 + pathCard × 0.01
     = INDEX_SETUP + traversalIO + dataIO + cpuCost
```
Where `trieDepth = log16(pathCardinality)` (HOT effective fanout ~16).

**Selective index scan** (with predicate):
```
effectiveCard = pathCardinality × selectivity
cost = 3.0 + traversalIO(full) + ceil(effectiveCard/512) × 4.0 + effectiveCard × 0.01
```

### Selectivity Estimator (`SelectivityEstimator.java`)

| Predicate Type | Default | With Histogram |
|---------------|---------|---------------|
| Equality (`=`) | 1% | MCV exact frequency, or (1-mcvFrac)/(NDV-mcvCount) |
| Range (`<`, `>`) | 33% | Bucket overlap fraction + MCV contribution |
| LIKE | 25% | — |
| AND | SQL Server exponential backoff: `s0 × s1^(1/2) × s2^(1/3) × ...` | Same formula, per-predicate histogram |
| OR | Inclusion-exclusion: `P(A∪B) = P(A) + P(B) - P(A)·P(B)` | Same |
| NOT | `1 - selectivity(child)` | Same |

### Cardinality Estimator (`CardinalityEstimator.java`)

Walks the FLWOR pipeline and annotates each node:

| Node Type | Formula |
|-----------|---------|
| ForBind | `inputCard × bindingCard` |
| LetBind | `inputCard` (preserves) |
| Selection | `inputCard × selectivity(predicate)` |
| GroupBy | `distinctCount` (from histogram) or `inputCard / 100` |
| OrderBy | `inputCard` (preserves) |
| Join | `leftCard × rightCard × 0.1` |

### Auto-Collection on Cache Miss

When `StatisticsCatalog` has no histogram for a queried field, `CostBasedStage` records the field in `pendingCollections`. After optimization completes, `SirixOptimizer` (or callers via `SirixCompileChain.collectPendingHistograms()`) can trigger deferred collection via `HistogramCollector`. This separation avoids deadlocks from opening resource sessions during an active write transaction.

---

## 6. Stage 3: Join Reordering

**`JoinReorderStage`** (`JoinReorderStage.java:40`) delegates to `CostBasedJoinReorder` which:

1. **Extracts join groups** from fused join annotations (Rule 1)
2. **Builds a `JoinGraph`** with base relation cardinalities and join edges
3. **Runs the adaptive algorithm** (`AdaptiveJoinOrderOptimizer.java`):
   - **DPhyp** for ≤20 relations: O(n·3^n) optimal enumeration of connected subgraph complement pairs, memoized in `DpTable` (bitmask → best plan)
   - **GOO** for >20 relations: O(n^3) Greedy Operator Ordering fallback
4. **Restructures the AST**: swaps join children to match the optimal order, annotates with `JOIN_REORDERED=true`

### DPhyp Algorithm (`AdaptiveJoinOrderOptimizer.java`)

Phase 1: Initialize base plans (one per relation) with cardinality and cost.

Phase 2: For each relation (high bitmask to low):
  - Enumerate connected subgraphs S1 containing this relation
  - For each S1, find complement pairs S2 such that S1∪S2 is connected
  - Compute join cost for (S1, S2), update DP table if cheaper

The `DpTable` (`DpTable.java`) uses open-addressing with Fibonacci hashing for cache-friendly lookups on bitmask keys.

---

## 7. Stages 4-5: Mesh Population & Selection

### Stage 4: MeshPopulationStage (`MeshPopulationStage.java`)

The **Mesh** (Graefe/DeWitt Exodus) groups semantically equivalent plans into `EquivalenceClass` instances:

For each ForBind/LetBind with cost annotations:
- Creates equivalence class with **seq scan** as first alternative (cost: `SEQ_SCAN_COST`)
- Adds **index scan** alternative if available (cost: `INDEX_SCAN_COST`)
- Tags AST node with `MESH_CLASS_ID`

For each Join with cost annotations:
- Creates equivalence class with current order
- Adds swapped-order alternative if NLJ cost differs
- Links child classes via virtual edges (`setChildClasses()`)

### Stage 5: MeshSelectionStage (`MeshSelectionStage.java`)

**Post-order walk** (bottom-up): resolves children before parents so child costs inform parent decisions.

For each node with `MESH_CLASS_ID`:
1. Retrieves best plan from Mesh
2. Copies decision properties (`PREFER_INDEX`, `INDEX_ID`, costs)
3. For joins: if best plan is an alternative, **swaps children** to match preferred order
4. Propagates child costs to parent join cost

---

## 8. Stage 6: Index Decomposition

**`IndexDecompositionStage`** (`IndexDecompositionStage.java:23`) runs a two-phase process:

**Phase 1**: `JoinDecompositionWalker` annotates joins with decomposition metadata:
- **Rule 5**: One join side has an index, the other doesn't → `DECOMPOSITION_TYPE=RULE_5`
- **Rule 6**: Both sides have different indexes → `DECOMPOSITION_TYPE=RULE_6`, `DECOMPOSITION_INTERSECT=true`

**Phase 2**: Post-order AST restructuring reads annotations and mutates:
- **Rule 5** (`applyRule5`, line 90): Swaps join children so the indexed side is child 0 (driving side). Propagates `PREFER_INDEX=true`.
- **Rule 6** (`applyRule6`, line 125): Marks as `INTERSECTION_JOIN=true`, ensures both sides have `PREFER_INDEX=true`.

Idempotency guard: `DECOMPOSITION_RESTRUCTURED=true` prevents double-processing.

---

## 9. Stage 7: Cost-Driven Routing

**`CostDrivenRoutingStage`** (`CostDrivenRoutingStage.java:28`) translates high-level cost decisions into gate signals for index matching:

When a ForBind's binding has `PREFER_INDEX=false` (sequential scan is cheaper), propagates `INDEX_GATE_CLOSED=true` to all descendants. Index matching walkers in Stage 10 check this gate before applying rewrites.

---

## 10. Stages 8-9: Vectorized Execution

### Stage 8: VectorizedDetectionStage

Detects scan-filter-project pipelines eligible for SIMD execution: ForBind over `jn:doc()` with simple predicates and field projections.

### Stage 9: VectorizedRoutingStage (`VectorizedRoutingStage.java`)

The largest single stage (458 lines). Routes detected pipelines to columnar execution:
- Creates `VectorizedPipelineExpr` AST nodes replacing the FLWOR subpipeline
- Configures batch size, filter types (string equality via FSST, range predicates)
- Falls back to tuple-at-a-time when SIMD isn't beneficial

---

## 11. Stage 10: Index Matching

**`IndexMatching`** (inner class in `SirixOptimizer.java:252`) runs three walkers in priority order:

1. **`JsonCASStep`** (327 lines): Matches CAS (Content And Structure) B+-tree indexes. Extracts comparison predicates, maps to `SearchMode` (EQUAL, GREATER, LOWER, etc.), creates `IndexExpr` AST nodes.

2. **`JsonPathStep`** (75 lines): Matches PATH indexes for field access patterns.

3. **`JsonObjectKeyNameStep`** (76 lines): Matches NAME indexes on object keys.

All three extend `AbstractJsonPathWalker` (544 lines) which handles:
- Path extraction from deref chains
- PathSummary validation (does the path exist?)
- Cost gate checking (`CostProperties.isIndexGateClosed()`)
- Index definition lookup via `IndexController`

When a match is found, the walker replaces the original AST subtree with an `IndexExpr` node containing the index definition, database/resource metadata, and filter parameters.

---

## 12. Statistics Infrastructure

### Histogram (`Histogram.java`)

Dual-mode histogram with MCV tracking:

**Equi-width** (default): Fixed-width buckets, O(1) bucket lookup via arithmetic. Good for uniform distributions.

**Equi-depth**: Equal-count buckets, O(log B) lookup via binary search on `bucketBoundaries[]`. Better for skewed distributions — provides uniform estimation error across the value range.

**MCV (Most-Common-Values)**: Top-K most frequent values (default K=10) stored separately with exact frequencies. Bucket counts are reduced by MCV frequencies during construction to prevent double-counting. For equality predicates, MCVs provide exact selectivity; for non-MCVs, the remaining fraction is distributed across remaining distinct values.

### StatisticsCatalog (`StatisticsCatalog.java`)

Singleton LRU cache mapping `(database, resource, path, revision)` to histograms:
- **Historical revisions** (revision > 0): Never expire via TTL — immutable data means histograms are forever valid.
- **Latest revision** (revision = -1): Subject to 1-hour TTL and write-triggered invalidation.
- Max 4096 entries with LRU eviction.

### HistogramCollector (`HistogramCollector.java`)

Analogous to SQL's ANALYZE command:
1. Queries PathSummary for path existence
2. Opens read-only transaction
3. Stride-based sampling: `stride = totalNodes / (sampleSize × 2)`
4. Collects numeric values, tracks distinct count
5. Builds histogram via `Histogram.Builder`

### Post-Commit Invalidation

`SirixQueryContext.invalidateStatisticsForResource()` fires after each commit:
- Invalidates `StatisticsCatalog` entries for the modified resource (latest revision only)
- Signals `PlanCache` schema change to invalidate all cached plans

---

## 13. Adaptive Re-Optimization

**`CardinalityTracker`** (`CardinalityTracker.java`) implements dampened drift detection:

- Records `(cacheKey, estimatedCardinality, actualCardinality)` per query execution
- Computes ratio: `max(estimated/actual, actual/estimated)`
- **Threshold**: 10x mismatch
- **Damping**: 3 consecutive mismatched executions required before invalidation
- On trigger: surgical plan cache invalidation (specific query, not global)
- Accurate estimates reset the consecutive counter — a single outlier doesn't overreact

---

## 14. Plan Cache

**`PlanCache`** (`PlanCache.java`) — LRU cache for optimized ASTs:

- **Cache key**: `queryText + "@v" + indexSchemaVersion`
- **Version-aware**: When indexes are created/dropped, `signalIndexSchemaChange()` increments a global `AtomicLong`, making all previously cached plans stale
- **Deep copy safety**: `get()` returns `copyTree()` to prevent downstream mutation; `put()` stores a deep copy
- **Capacity**: 128 plans (LRU eviction)
- **Metrics**: hit/miss counters, hit ratio

---

## 15. EXPLAIN Function & QueryPlan API

### sdb:explain() Function (`Explain.java`)

Three signatures:
```xquery
sdb:explain($query)                    -- optimized plan JSON
sdb:explain($query, true())            -- parsed + optimized
sdb:explain($query, 'candidates')      -- optimized + Mesh alternatives
```

### QueryPlan Record (`QueryPlan.java`)

Programmatic API for plan inspection:
```java
QueryPlan plan = QueryPlan.explain(query, jsonStore, xmlStore);
plan.usesIndex()                  // IndexExpr present (actual rewrite)
plan.prefersIndex()               // PREFER_INDEX=true (cost model hint)
plan.indexType()                  // "CAS", "PATH", "NAME", or null
plan.estimatedCardinality()       // from ESTIMATED_CARDINALITY
plan.isIntersectionJoin()         // Rule 6 intersection
plan.isDecompositionRestructured() // Rules 5-6 restructuring
plan.isJoinReordered()            // DPhyp/GOO applied
plan.hasClosedGate()              // INDEX_GATE_CLOSED
plan.vectorizedRoute()            // "columnar" or null
plan.toJSON()                     // pretty-printed plan
plan.toCandidatesJSON()           // plan + Mesh alternatives
```

### QueryPlanSerializer (`QueryPlanSerializer.java`)

Converts AST to human-readable JSON with operator names, property annotations, and tree structure. Handles all XQ.* node types with meaningful labels.

---

## 16. Runtime Integration

### SirixPipelineStrategy (`SirixPipelineStrategy.java`)

Extends Brackit's `SequentialPipelineStrategy` to support optimizer annotations at the physical operator level:

When `INTERSECTION_JOIN=true` is detected on a Join node, forces `skipSort=true` on the `TableJoin` operator. This skips the O(n log n) post-probe sort+dedup — correct for intersection joins since index results are already unique by nodeKey.

### IndexExpr (`IndexExpr.java`)

Physical operator for index-based execution. Evaluates by:
1. Opening resource session and transaction
2. Dispatching on index type (PATH, CAS, NAME)
3. Opening the appropriate index via `IndexController`
4. Filtering and materializing results into `ItemSequence`

### VectorizedPipelineExpr (`VectorizedPipelineExpr.java`)

Physical operator for columnar SIMD execution:
1. Opens columnar scan (batch-oriented)
2. Applies SIMD string filters (FSST-aware)
3. Applies range predicates
4. Materializes surviving rows

---

## 17. Testing Strategy

**33 test files, ~8,300 lines** organized in four tiers:

### Tier 1: Unit Tests (per-component)
- `HistogramTest.java` (601 lines) — equi-width, equi-depth, MCV, edge cases
- `SelectivityEstimatorTest.java` — AND/OR/NOT formulas
- `CardinalityEstimatorTest.java` — FLWOR pipeline propagation
- `JsonCostModelTest.java` — cost comparison correctness
- `DPhypJoinOrdererTest.java` (433 lines) — DPhyp + GOO algorithms
- `DpTableTest.java` — hash table correctness
- `MeshTest.java` — equivalence class operations
- `CardinalityTrackerTest.java` — drift detection and damping

### Tier 2: Stage Integration Tests
- `CostBasedStageTest.java` — annotation correctness
- `MeshPopulationStageTest.java` — alternative creation
- `MeshSelectionStageTest.java` (401 lines) — bottom-up selection, join swap
- `IndexDecompositionStageTest.java` (379 lines) — Rules 5-6 restructuring
- `CostDrivenRoutingTest.java` — gate propagation
- `VectorizedRoutingStageTest.java` (364 lines) — SIMD pipeline routing

### Tier 3: Walker Tests
- `JoinFusionWalkerTest.java` — Rule 1 annotation
- `SelectAccessFusionWalkerTest.java` — Rule 3 predicate pushdown
- `SelectJoinFusionWalkerTest.java` — Rule 2 annotation
- `JoinCommutativityWalkerTest.java` — Rule 4 swap
- `JoinDecompositionWalkerTest.java` — Rules 5-6 annotation

### Tier 4: End-to-End Tests
- `OptimizerBehavioralE2ETest.java` (793 lines) — **21 tests** storing real data, creating indexes, verifying both plan decisions AND exact query results
- `ExplainIntegrationTest.java` — sdb:explain() through full pipeline
- `OptimizerProductionReadinessTest.java` (603 lines) — timeouts, GOO fallback, cycle detection, statistics lifecycle, plan cache invalidation

---

## 18. References

1. **Weiner et al.** "Cost-Based Optimization of Integration Flows" (TU Kaiserslautern, CEUR-WS 2008) — XQGM architecture, 6 rewrite rules, Mesh search space
2. **Moerkotte & Neumann** "Analysis of Two Existing and One New Dynamic Programming Algorithm for the Generation of Optimal Bushy Join Trees without Cross Products" (VLDB 2006) — DPhyp algorithm
3. **Neumann & Radke** "Adaptive Optimization of Very Large Join Queries" (SIGMOD 2018) — GOO fallback for large join graphs
4. **Graefe & DeWitt** "The EXODUS Optimizer Generator" (SIGMOD 1987) — Mesh/equivalence class search space
5. **PostgreSQL** `src/backend/optimizer/path/costsize.c` — Selectivity defaults, exponential backoff for AND, equi-depth histograms, MCV tracking

---

## Code Tour: File Map

```
bundles/sirix-query/src/main/java/io/sirix/query/
├── SirixCompileChain.java              ← Entry point: compile chain
├── SirixQueryContext.java              ← Post-commit invalidation
├── compiler/
│   ├── XQExt.java                      ← Extension AST node types
│   ├── translator/
│   │   ├── SirixTranslator.java        ← AST → physical operators
│   │   └── SirixPipelineStrategy.java  ← Intersection join support
│   ├── expression/
│   │   ├── IndexExpr.java              ← Index scan physical operator
│   │   └── VectorizedPipelineExpr.java ← SIMD pipeline operator
│   └── optimizer/
│       ├── SirixOptimizer.java         ← 10-stage pipeline orchestrator
│       ├── PlanCache.java              ← LRU plan cache
│       ├── CardinalityTracker.java     ← Adaptive re-optimization
│       ├── JqgmRewriteStage.java       ← Stage 1: Rules 1-3
│       ├── CostBasedStage.java         ← Stage 2: Cost analysis
│       ├── JoinReorderStage.java       ← Stage 3: DPhyp
│       ├── MeshPopulationStage.java    ← Stage 4: Build search space
│       ├── MeshSelectionStage.java     ← Stage 5: Select best plan
│       ├── IndexDecompositionStage.java ← Stage 6: Rules 5-6
│       ├── CostDrivenRoutingStage.java ← Stage 7: Gate signals
│       ├── VectorizedDetectionStage.java ← Stage 8: SIMD detection
│       ├── VectorizedRoutingStage.java ← Stage 9: SIMD routing
│       ├── join/
│       │   ├── AdaptiveJoinOrderOptimizer.java ← DPhyp + GOO
│       │   ├── JoinGraph.java          ← Dense join graph
│       │   ├── JoinPlan.java           ← Plan tree node
│       │   ├── JoinEdge.java           ← Graph edge
│       │   └── DpTable.java            ← DP memoization
│       ├── mesh/
│       │   ├── Mesh.java               ← Exodus search space
│       │   ├── EquivalenceClass.java   ← Plan group
│       │   └── PlanAlternative.java    ← Single alternative
│       ├── stats/
│       │   ├── CostProperties.java     ← 40+ property key constants
│       │   ├── JsonCostModel.java      ← I/O + CPU cost model
│       │   ├── SelectivityEstimator.java ← Predicate selectivity
│       │   ├── CardinalityEstimator.java ← Pipeline cardinality
│       │   ├── Histogram.java          ← Equi-width/depth + MCV
│       │   ├── HistogramCollector.java ← ANALYZE command
│       │   ├── StatisticsCatalog.java  ← Revision-aware histogram cache
│       │   ├── SirixStatisticsProvider.java ← PathSummary-backed stats
│       │   ├── StatisticsProvider.java ← Interface
│       │   ├── IndexInfo.java          ← Index metadata
│       │   └── BaseProfile.java        ← Physical profile
│       └── walker/
│           └── json/
│               ├── JoinFusionWalker.java      ← Rule 1
│               ├── SelectJoinFusionWalker.java ← Rule 2
│               ├── SelectAccessFusionWalker.java ← Rule 3
│               ├── JoinCommutativityWalker.java  ← Rule 4
│               ├── JoinDecompositionWalker.java  ← Rules 5-6 (annotation)
│               ├── CostBasedJoinReorder.java     ← Join group extraction
│               ├── AbstractJsonPathWalker.java   ← Index matching base
│               ├── JsonCASStep.java              ← CAS index matching
│               ├── JsonPathStep.java             ← PATH index matching
│               └── JsonObjectKeyNameStep.java    ← NAME index matching
└── function/sdb/explain/
    ├── Explain.java                    ← sdb:explain() function
    ├── QueryPlan.java                  ← Plan inspection API
    └── QueryPlanSerializer.java        ← AST → JSON serialization
```
