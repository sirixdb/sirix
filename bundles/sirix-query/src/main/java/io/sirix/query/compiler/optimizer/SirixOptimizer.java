package io.sirix.query.compiler.optimizer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.walker.json.JsonPathStep;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.compiler.optimizer.TopDownOptimizer;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.json.JsonCASStep;
import io.sirix.query.compiler.optimizer.walker.json.JsonObjectKeyNameStep;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.XmlDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SirixOptimizer extends TopDownOptimizer {

  private static final Logger LOG = LoggerFactory.getLogger(SirixOptimizer.class);

  /** Maximum time allowed for the full optimization pipeline before circuit-breaking. */
  private static final long OPTIMIZATION_TIMEOUT_MS = 50L;

  /** Pre-computed timeout in nanoseconds to avoid repeated conversion. */
  private static final long OPTIMIZATION_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(OPTIMIZATION_TIMEOUT_MS);

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
    // 10. Perform index matching as last step.
    getStages().add(new IndexMatching(nodeStore, jsonItemStore));
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
    // Circuit breaker: if total optimization time exceeds the timeout, return
    // the partially-optimized AST to avoid blocking query execution.
    final long startNanos = System.nanoTime();
    AST current = ast;
    for (final Stage stage : getStages()) {
      if (!disabledStages.contains(stage.getClass())) {
        current = stage.rewrite(sctx, current);
      }
      final long elapsedNanos = System.nanoTime() - startNanos;
      if (elapsedNanos > OPTIMIZATION_TIMEOUT_NS) {
        LOG.warn("Optimization pipeline exceeded {}ms timeout after stage {} (elapsed {}ms), "
                + "returning partially-optimized AST",
            OPTIMIZATION_TIMEOUT_MS,
            stage.getClass().getSimpleName(),
            TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
        break;
      }
    }
    final AST optimized = current;

    // Cache a deep copy so the caller's subsequent mutations don't corrupt the entry.
    if (cacheKey != null) {
      planCache.put(cacheKey, optimized.copyTree());
    }

    return optimized;
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
    // Insert before last stage (IndexMatching)
    final int lastIndex = getStages().size() - 1;
    if (lastIndex >= 0) {
      getStages().add(lastIndex, stage);
    } else {
      getStages().add(stage);
    }
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

  private static class IndexMatching implements Stage {
    private final XmlDBStore xmlNodeStore;

    private final JsonDBStore jsonItemStore;

    public IndexMatching(final XmlDBStore xmlNodestore, final JsonDBStore jsonItemStore) {
      this.xmlNodeStore = xmlNodestore;
      this.jsonItemStore = jsonItemStore;
    }

    @Override
    public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
      ast = new JsonCASStep(jsonItemStore).walk(ast);
      ast = new JsonPathStep(jsonItemStore).walk(ast);
      ast = new JsonObjectKeyNameStep(jsonItemStore).walk(ast);

      return ast;
    }
  }
}
