package io.sirix.query.compiler.optimizer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.walker.json.JsonPathStep;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.compiler.optimizer.TopDownOptimizer;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.json.JsonCASStep;
import io.sirix.query.compiler.optimizer.walker.json.JsonObjectKeyNameStep;
import io.sirix.query.compiler.optimizer.walker.json.JsonValidTimeStep;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.XmlDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SirixOptimizer extends TopDownOptimizer {

  private static final Logger LOG = LoggerFactory.getLogger(SirixOptimizer.class);

  /**
   * Join-relation count above which the exploratory join-reorder / mesh stages (those marked
   * {@link BudgetSheddable}) are shed for a bounded compile.
   *
   * <p>This replaces a former 50ms WALL-CLOCK circuit breaker whose timing-dependent shedding could
   * drop a stage — and therefore change which indexes a query used — based purely on runner speed
   * (the flaky-plan bug this fixes). The exploratory stage whose cost grows fastest is DPhyp join
   * reordering, O(n&#183;3&#8319;) in the join-relation count n. The default 12 is derived from that
   * bound: n&#183;3&#8319; &#8776; 6.4M at n=12 (sub-millisecond), so shedding above it keeps the
   * join search cheap. It is deliberately BELOW
   * {@code AdaptiveJoinOrderOptimizer.DPHYP_THRESHOLD} (20, above which that optimizer falls back to
   * greedy GOO): at n=20 DPhyp alone is n&#183;3&#8319; &#8776; 70e9 (seconds), so the internal
   * threshold is not by itself a compile-time guard — this budget is. If you change either constant,
   * review the other.</p>
   *
   * <p>The count is a deliberately CONSERVATIVE global upper bound on any single connected join
   * group's arity (the quantity DPhyp is actually exponential in): a query with several independent
   * small join groups may be shed although no one group is large. That only costs join-order quality
   * (results stay correct); it never lets a blowup through. A per-connected-group count is a possible
   * refinement.</p>
   *
   * <p>DETERMINISM CAVEAT: unlike the wall-clock breaker, this budget does NOT cap total compile
   * time — only the exploratory join/mesh search. The always-run stages (cost estimation, the four
   * index-matching walkers) are each O(query) but uncapped, so a pathological low-join query with
   * very many distinct paths / predicates can still compile slowly. That is the accepted price of a
   * deterministic plan; the wall-clock net that previously bounded such cases is intentionally gone.</p>
   */
  private static final int MAX_JOIN_RELATIONS_FOR_REORDER =
      Integer.getInteger("sirix.optimizer.maxJoinRelations", 12);

  private final XmlDBStore xmlNodeStore;
  private final JsonDBStore jsonItemStore;
  private final PlanCache planCache;
  private final Set<Class<? extends Stage>> disabledStages = new HashSet<>(4);
  private final Mesh mesh;
  private final CostBasedStage costBasedStage;

  public SirixOptimizer(final Map<QNm, Str> options, final XmlDBStore nodeStore, final JsonDBStore jsonItemStore) {
    this(options, nodeStore, jsonItemStore, new PlanCache());
  }

  public SirixOptimizer(final Map<QNm, Str> options, final XmlDBStore nodeStore,
                         final JsonDBStore jsonItemStore, final PlanCache planCache) {
    super(options);
    this.xmlNodeStore = nodeStore;
    this.jsonItemStore = jsonItemStore;
    this.planCache = planCache;
    // 1. JQGM rewrite rules (Rules 1-4) — predicate pushdown and join fusion before cost analysis.
    getStages().add(new JqgmRewriteStage());
    // 2. Cost-based optimization: annotate AST with index preference hints and cardinality estimates.
    this.costBasedStage = new CostBasedStage(jsonItemStore);
    getStages().add(costBasedStage);
    // 3. DPhyp-based join reordering — uses cardinality estimates to find optimal join orders.
    getStages().add(new JoinReorderStage());
    // 4. Populate Mesh search space with plan alternatives from cost annotations.
    this.mesh = new Mesh(32);
    getStages().add(new MeshPopulationStage(mesh));
    // 5. Apply best-plan decisions from the shared Mesh to the original AST.
    getStages().add(new MeshSelectionStage(mesh));
    // 6. Index-aware join decomposition (Rules 5-6) — splits joins at index boundaries.
    getStages().add(new IndexDecompositionStage());
    // 7. Cost-driven execution routing — propagate PREFER_INDEX to downstream index matching.
    getStages().add(new CostDrivenRoutingStage());
    // 8. + 9. Vectorized detection/routing — DISABLED. Brackit's optimizer
    // already runs VectorizedGroupByDetection (parent constructor adds it),
    // and SirixVectorizedExecutor implements Brackit's VectorizedExecutor SPI.
    // The two pipelines competed: Sirix's routing replaced the PipeExpr AST
    // before Brackit's dispatch ran, so the Brackit-side executor was never
    // called. Falling back to Brackit's SPI lets a registered
    // SirixVectorizedExecutor handle group-by/aggregate/filter-count, while
    // unregistered queries still go through Volcano (the original behavior).
    // getStages().add(new VectorizedDetectionStage());
    // getStages().add(new VectorizedRoutingStage());
    // 9b. Sirix-side per-group aggregate detection (P5b stage 7a): annotates the
    // group-by-with-aggregates pipe shape Brackit's walker doesn't cover; consumed by
    // SirixPipelineStrategy. Runs AFTER Brackit's VectorizedGroupByDetection (parent
    // constructor) because it reuses its predicate/source-path annotations.
    getStages().add(new GroupAggregateDetectionStage());
    // 9c. Covered-row detection (P5b stage 7c): record-constructor returns over covered
    // fields, servable from projection segments alone. Same ordering rationale as 9b.
    getStages().add(new RowMaterializeDetectionStage());
    // 9d. Sorted-scan detection (P5b stage 7b, gap 1b): 1..N order-by keys with per-spec
    // direction, read straight from the OrderBy AST (Brackit's single-key annotation and
    // its unusable direction property are no longer consumed). Same ordering rationale.
    getStages().add(new SortedScanDetectionStage());
    // 9e. Computed-expression aggregate detection (gap 2): +/-/* trees over covered
    // numeric fields inside sum/avg/min/max/count, compiled to a postfix program the
    // exact-arithmetic kernel folds; consumed by SirixTranslator's functionCall seam.
    getStages().add(new ComputedAggregateDetectionStage());
    // 10. Index matching as the last step. It always runs (never budget-shed): the index decision
    //     is authored solely by the always-run CostBasedStage (mesh only re-derives it, never
    //     contradicts it — so shedding join/mesh cannot change which indexes a query uses), and
    //     applying that decision is cheap. Keeping it mandatory is what makes index selection
    //     independent of the optimizer budget.
    getStages().add(new IndexMatching(jsonItemStore));
  }

  @Override
  public AST optimize(StaticContext sctx, AST ast) {
    // Include index schema version in cache key so that plans are invalidated
    // when indexes are created or dropped (prevents stale index/scan decisions).
    final String queryText = ast.toString();
    final String cacheKey = queryText != null
        ? queryText + "@v" + PlanCache.indexSchemaVersion()
        : null;
    if (cacheKey != null) {
      final AST cached = planCache.get(cacheKey);
      if (cached != null) {
        // PlanCache.get() already returns a deep copy — no need to copy again.
        return cached;
      }
    }

    // Run stages inline (instead of super.optimize()) to support disabled stages.
    // Deterministic budget: for a query with more join relations than the threshold, shed the
    // exploratory join-reorder / mesh stages (BudgetSheddable). The decision depends only on the
    // query shape — identical on every machine — so the cost stages and index matching always run
    // and index/plan selection never depends on runner speed. The decision is taken LAZILY on the
    // first sheddable stage, i.e. after the always-run JQGM rewrite has formed the join structure —
    // counting the raw parsed AST would miss joins JQGM is about to create.
    Boolean shedExploratory = null;
    AST current = ast;
    for (final Stage stage : getStages()) {
      if (disabledStages.contains(stage.getClass())) {
        continue;
      }
      if (stage instanceof BudgetSheddable) {
        if (shedExploratory == null) {
          shedExploratory = joinRelationCountExceeds(current, MAX_JOIN_RELATIONS_FOR_REORDER);
          if (shedExploratory && LOG.isDebugEnabled()) {
            LOG.debug("Query exceeds {} join relations; shedding exploratory join-reorder/mesh "
                + "stages for a bounded, deterministic compile", MAX_JOIN_RELATIONS_FOR_REORDER);
          }
        }
        if (shedExploratory) {
          continue;
        }
      }
      current = stage.rewrite(sctx, current);
    }
    final AST optimized = current;

    // Cache a deep copy so the caller's subsequent mutations don't corrupt the entry.
    if (cacheKey != null) {
      planCache.put(cacheKey, optimized.copyTree());
    }

    return optimized;
  }

  /**
   * Whether the AST holds more than {@code threshold} join relations ({@code ForBind}/{@code Join}
   * nodes) — the input size the DPhyp join-reorder cost is exponential in. Deterministic and
   * identical on every machine, so the shed decision it drives never depends on runner speed.
   * Short-circuits as soon as the threshold is passed (callers only need the boolean).
   */
  private static boolean joinRelationCountExceeds(final AST node, final int threshold) {
    return countJoinRelations(node, threshold) > threshold;
  }

  /** ForBind+Join count, capped at {@code threshold + 1} via early-exit. */
  private static int countJoinRelations(final AST node, final int threshold) {
    int count = node.getType() == XQ.ForBind || node.getType() == XQ.Join ? 1 : 0;
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      if (count > threshold) {
        return count;
      }
      count += countJoinRelations(node.getChild(i), threshold);
    }
    return count;
  }

  /**
   * Get the plan cache.
   *
   * @return The plan cache
   */
  public PlanCache getPlanCache() {
    return planCache;
  }

  /**
   * Get the Mesh containing plan alternatives from the last optimization run.
   *
   * @return the Mesh search space
   */
  public Mesh getMesh() {
    return mesh;
  }

  /**
   * Collect histograms for fields that had cache misses during the last optimization.
   * Call this after query execution completes and all resource sessions are closed.
   */
  public void collectPendingHistograms() {
    costBasedStage.collectPendingHistograms();
  }

  /**
   * Get the JSON database store.
   *
   * @return The JSON database store
   */
  protected JsonDBStore getJsonDBStore() {
    return jsonItemStore;
  }

  /**
   * Get the XML node store.
   *
   * @return The XML node store
   */
  protected XmlDBStore getXmlDBStore() {
    return xmlNodeStore;
  }

  /**
   * Add an optimization stage at a specific position.
   *
   * <p>
   * This method allows subclasses (e.g., sirix-enterprise) to inject custom optimization stages at
   * any position in the optimization pipeline.
   * </p>
   *
   * @param index Position to insert the stage (0-based)
   * @param stage The optimization stage to add
   */
  protected void addStageAt(int index, Stage stage) {
    getStages().add(index, stage);
  }

  /**
   * Add an optimization stage before the index matching stage (at the end).
   *
   * <p>
   * This is a convenience method for adding stages that should run after all other optimizations but
   * before index matching.
   * </p>
   *
   * @param stage The optimization stage to add
   */
  protected void addStageBeforeIndexMatching(Stage stage) {
    // Insert before the index-matching stage so injected stages run before index matching.
    final var stages = getStages();
    for (int i = 0; i < stages.size(); i++) {
      if (stages.get(i) instanceof IndexMatchingStage) {
        stages.add(i, stage);
        return;
      }
    }
    stages.add(stage);
  }

  /**
   * Add an optimization stage at the beginning of the pipeline.
   *
   * <p>
   * Stages added here will run first, before any other optimization.
   * </p>
   *
   * @param stage The optimization stage to add
   */
  protected void addStageFirst(Stage stage) {
    getStages().add(0, stage);
  }

  /**
   * Get the number of optimization stages.
   *
   * @return Number of stages currently in the pipeline
   */
  protected int getStageCount() {
    return getStages().size();
  }

  /**
   * Disable an optimization stage by its class.
   *
   * <p>Disabled stages are skipped during optimization. This allows enterprise
   * subclasses or tests to selectively turn off stages without removing them
   * from the pipeline (preserving stage ordering for later re-enable).</p>
   *
   * @param stageClass the stage class to disable
   */
  public void disableStage(Class<? extends Stage> stageClass) {
    disabledStages.add(stageClass);
  }

  /**
   * Re-enable a previously disabled optimization stage.
   *
   * @param stageClass the stage class to enable
   */
  public void enableStage(Class<? extends Stage> stageClass) {
    disabledStages.remove(stageClass);
  }

  /**
   * Check if a stage is currently enabled.
   *
   * @param stageClass the stage class to check
   * @return true if the stage is enabled (not in the disabled set)
   */
  public boolean isStageEnabled(Class<? extends Stage> stageClass) {
    return !disabledStages.contains(stageClass);
  }

  /**
   * Marker type for the index-matching stage, so {@link #addStageBeforeIndexMatching} can locate it.
   */
  private interface IndexMatchingStage extends Stage {
  }

  /**
   * Applies the index rewrites (valid-time, CAS, path, object-key). Each walker consults the cost
   * gate ({@code INDEX_GATE_CLOSED}, authored by the always-run {@link CostBasedStage}) except
   * valid-time, which currently matches structurally; either way the decision is made by the
   * always-run cost stage, so this stage is NOT {@link BudgetSheddable} — it always runs, keeping
   * index selection independent of the budget.
   */
  private static final class IndexMatching implements IndexMatchingStage {
    private final JsonDBStore jsonItemStore;

    private IndexMatching(final JsonDBStore jsonItemStore) {
      this.jsonItemStore = jsonItemStore;
    }

    @Override
    public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
      // Valid-time FIRST: it consumes a FLWOR stabbing predicate into a jn:scan-valid-time-index
      // call before the CAS path inspects FilterExprs. Each walker is narrowly scoped and leaves
      // every non-matching query's AST untouched.
      ast = new JsonValidTimeStep(jsonItemStore).walk(ast);
      ast = new JsonCASStep(jsonItemStore).walk(ast);
      ast = new JsonPathStep(jsonItemStore).walk(ast);
      ast = new JsonObjectKeyNameStep(jsonItemStore).walk(ast);
      return ast;
    }
  }
}
