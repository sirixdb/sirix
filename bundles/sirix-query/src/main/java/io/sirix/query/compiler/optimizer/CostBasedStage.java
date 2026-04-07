package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.path.Path;
import io.sirix.query.compiler.optimizer.stats.CardinalityEstimator;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.stats.Histogram;
import io.sirix.query.compiler.optimizer.stats.IndexInfo;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import io.sirix.query.compiler.optimizer.stats.SelectivityEstimator;
import io.sirix.query.compiler.optimizer.stats.SirixStatisticsProvider;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import io.sirix.query.compiler.optimizer.stats.StatisticsProvider;
import io.sirix.query.compiler.optimizer.stats.HistogramCollector;
import io.sirix.query.json.JsonDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Cost-based optimization stage that annotates AST nodes with index preference hints.
 *
 * <p>For each path access that has an available index, this stage compares
 * the estimated cost of a sequential scan vs an index scan using real
 * cardinality statistics from PathSummary. If the index scan is cheaper,
 * it annotates the AST node with {@code costBased.preferIndex=true} and
 * the index metadata.</p>
 *
 * <p>This runs before the existing IndexMatching stage, providing cost-aware
 * guidance for index selection. When no cost data is available, the stage
 * is a no-op and IndexMatching falls back to its existing heuristic behavior.</p>
 *
 * <p>Phase 1 of the cost-based query optimizer plan (Weiner et al. adaptation).</p>
 */
public final class CostBasedStage implements Stage {

  private static final Logger LOG = LoggerFactory.getLogger(CostBasedStage.class);

  private final JsonDBStore jsonStore;
  private final JsonCostModel costModel;
  private final SelectivityEstimator selectivityEstimator;
  private final CardinalityEstimator cardinalityEstimator;
  private SirixStatisticsProvider statsProvider;

  /**
   * Tracks which fields have already been collected in this optimization pass
   * to avoid re-collecting the same field multiple times.
   * Key format: "databaseName/resourceName/fieldPath". Cleared in {@link #rewrite}.
   */
  private final Set<String> pendingCollections = new HashSet<>(8);

  /** Lazy-initialized histogram collector for auto-collection on cache miss. */
  private HistogramCollector histogramCollector;

  /**
   * Tracks variable bindings for resolving VariableRef nodes.
   * Maps the variable's binding key (AST child(0) value) to its binding expression (child(1)).
   * Cleared between queries via rewrite().
   */
  private final Map<Object, AST> variableBindings = new HashMap<>(8);

  /**
   * Tracks which variable keys are currently being resolved in {@link #extractPathAndDocument}.
   * Prevents infinite loops from circular variable references (e.g., let $x := $y, let $y := $x).
   * Initialized per call to {@link #extractPathAndDocument}.
   */
  private Set<Object> activeVarResolutions;

  public CostBasedStage(JsonDBStore jsonStore) {
    this.jsonStore = jsonStore;
    this.costModel = new JsonCostModel();
    this.selectivityEstimator = new SelectivityEstimator();
    this.cardinalityEstimator = new CardinalityEstimator(selectivityEstimator);
  }

  /**
   * Get the statistics provider for sharing with other stages.
   */
  public StatisticsProvider getStatsProvider() {
    if (statsProvider == null) {
      statsProvider = new SirixStatisticsProvider(jsonStore);
    }
    return statsProvider;
  }

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    if (statsProvider == null) {
      statsProvider = new SirixStatisticsProvider(jsonStore);
    } else {
      statsProvider.clearCaches();
    }
    // Clear stale histogram from previous query to ensure fresh selectivity estimation
    selectivityEstimator.setHistogram(null);
    variableBindings.clear();
    pendingCollections.clear();

    try {
      annotateSubtree(ast);

      // Phase 2: Annotate cardinality estimates throughout the pipeline
      cardinalityEstimator.annotateCardinalities(ast);
    } catch (final Exception e) {
      // Ensure cached resource sessions are closed on exception path.
      // Without this, sessions opened by SirixStatisticsProvider during
      // annotateSubtree() would leak if an exception prevents clearCaches()
      // from being called on the next rewrite() invocation.
      statsProvider.clearCaches();
      throw e;
    }

    return ast;
  }

  /**
   * Get the selectivity estimator for sharing with other stages.
   */
  public SelectivityEstimator getSelectivityEstimator() {
    return selectivityEstimator;
  }

  /**
   * Get the cardinality estimator for sharing with other stages.
   */
  public CardinalityEstimator getCardinalityEstimator() {
    return cardinalityEstimator;
  }

  /**
   * Recursively walk the AST and annotate ForBind nodes that access
   * JSON paths with cost-based index preference hints.
   */
  private void annotateSubtree(AST node) {
    if (node == null) {
      return;
    }

    // Look for ForBind nodes that bind over a deref chain from jn:doc()
    if (node.getType() == XQ.ForBind || node.getType() == XQ.LetBind) {
      // Track variable binding for VariableRef resolution (Issue 2)
      if (node.getChildCount() >= 2) {
        final AST varDecl = node.getChild(0);
        final Object varKey = varDecl.getValue();
        if (varKey != null) {
          variableBindings.put(varKey, node.getChild(1));
        }
      }
      tryAnnotateBinding(node);
    }

    // Recurse into children
    for (int i = 0; i < node.getChildCount(); i++) {
      annotateSubtree(node.getChild(i));
    }
  }

  private void tryAnnotateBinding(AST bindNode) {
    // Extract the binding expression (the source of iteration)
    final AST bindingExpr;
    if (bindNode.getChildCount() >= 2) {
      bindingExpr = bindNode.getChild(1);
    } else {
      return;
    }

    // Walk down the expression to find a deref chain rooted at jn:doc()
    final var pathAndDoc = extractPathAndDocument(bindingExpr);
    if (pathAndDoc == null) {
      return;
    }

    final Path<QNm> path = pathAndDoc.path();
    final String databaseName = pathAndDoc.databaseName();
    final String resourceName = pathAndDoc.resourceName();
    final int revision = pathAndDoc.revision();

    // Look up histogram from the catalog for data-driven selectivity estimation.
    // Uses the leaf field name as the key — matches HistogramCollector's convention.
    final QNm tail = path.getLength() > 0 ? path.tail() : null;
    final String leafField = tail != null ? tail.getLocalName() : null;
    if (leafField != null) {
      // Revision-aware lookup: try the exact revision first, then fall back to
      // LATEST_REVISION. Historical revisions are immutable (never stale), while
      // latest revision entries are subject to TTL and write invalidation.
      // If the queried revision equals the most recent, it shares the LATEST entry.
      Histogram histogram = StatisticsCatalog.getInstance()
          .get(databaseName, resourceName, leafField, revision);
      if (histogram == null && revision != StatisticsCatalog.LATEST_REVISION) {
        // Fall back to LATEST_REVISION entry (covers revision == mostRecent case)
        histogram = StatisticsCatalog.getInstance()
            .get(databaseName, resourceName, leafField);
      }
      selectivityEstimator.setHistogram(histogram); // null clears previous

      // Lazy auto-collection: on cache miss, trigger histogram collection
      // so the current and subsequent queries benefit from data-driven estimates.
      if (histogram == null) {
        triggerAsyncHistogramCollection(databaseName, resourceName, leafField);
      }
    } else {
      selectivityEstimator.setHistogram(null);
    }

    // Get statistics
    final long pathCardinality = statsProvider.getPathCardinality(
        path, databaseName, resourceName, revision);
    final long totalNodeCount = statsProvider.getTotalNodeCount(
        databaseName, resourceName, revision);

    if (pathCardinality <= 0 || totalNodeCount <= 0) {
      return; // no stats available — don't annotate
    }

    // Always annotate cardinality info so downstream stages (join reorder, vectorized
    // routing) have statistics even when no index exists
    bindingExpr.setProperty(CostProperties.PATH_CARDINALITY, pathCardinality);
    bindingExpr.setProperty(CostProperties.TOTAL_NODE_COUNT, totalNodeCount);

    // Check if an index exists
    final IndexInfo indexInfo = statsProvider.getIndexInfo(
        path, databaseName, resourceName, revision);

    if (!indexInfo.exists()) {
      return; // no index — cardinality annotated above, nothing more to do
    }

    // Estimate predicate selectivity to refine index scan cost.
    // If a filter predicate narrows the result set (e.g., price > 9990),
    // the index only needs to scan pathCardinality × selectivity entries.
    final double selectivity = estimatePredicateSelectivity(bindNode, bindingExpr);

    // Cost comparison — use selectivity-adjusted cost when predicate is present
    final double seqScanCost = costModel.estimateSequentialScanCost(totalNodeCount);
    final double indexScanCost = selectivity < 1.0
        ? costModel.estimateSelectiveIndexScanCost(pathCardinality, selectivity)
        : costModel.estimateIndexScanCost(pathCardinality);

    if (costModel.isIndexScanCheaper(indexScanCost, seqScanCost)) {
      // Annotate the binding expression with cost-based hint
      bindingExpr.setProperty(CostProperties.PREFER_INDEX, true);
      bindingExpr.setProperty(CostProperties.INDEX_ID, indexInfo.indexId());
      bindingExpr.setProperty(CostProperties.INDEX_TYPE, indexInfo.type().name());
      bindingExpr.setProperty(CostProperties.INDEX_SCAN_COST, indexScanCost);
      bindingExpr.setProperty(CostProperties.SEQ_SCAN_COST, seqScanCost);
    } else {
      // Mark that we explicitly decided against the index
      bindingExpr.setProperty(CostProperties.PREFER_INDEX, false);
      bindingExpr.setProperty(CostProperties.SEQ_SCAN_COST, seqScanCost);
      bindingExpr.setProperty(CostProperties.INDEX_SCAN_COST, indexScanCost);
    }
  }

  /**
   * Estimate the selectivity of predicates associated with this binding.
   *
   * <p>Searches for predicates in two places:
   * <ol>
   *   <li>FilterExpr in the binding expression itself (e.g., {@code jn:doc(...)[].item[?$$.price gt X]})</li>
   *   <li>Sibling Selection nodes in the parent pipeline</li>
   * </ol>
   *
   * @return selectivity in (0, 1], or 1.0 if no predicate found
   */
  private double estimatePredicateSelectivity(AST bindNode, AST bindingExpr) {
    // Check if the binding expression itself is or contains a FilterExpr
    final AST filterPredicate = findFilterPredicate(bindingExpr, 5);
    if (filterPredicate != null) {
      return selectivityEstimator.estimateSelectivity(filterPredicate);
    }

    // Look for sibling Selection nodes in the parent FLWOR pipeline
    final AST parent = bindNode.getParent();
    if (parent != null) {
      for (int i = 0; i < parent.getChildCount(); i++) {
        final AST sibling = parent.getChild(i);
        if (sibling.getType() == XQ.Selection && sibling.getChildCount() > 0) {
          return selectivityEstimator.estimateSelectivity(sibling.getChild(0));
        }
      }
    }

    return 1.0; // no predicate found
  }

  /**
   * Search for a filter predicate in the expression subtree.
   * Returns the predicate AST node (child(1) of FilterExpr), or null.
   */
  private static AST findFilterPredicate(AST expr, int maxDepth) {
    if (expr == null || maxDepth <= 0) {
      return null;
    }
    if (expr.getType() == XQ.FilterExpr && expr.getChildCount() >= 2) {
      return expr.getChild(1);
    }
    for (int i = 0; i < expr.getChildCount(); i++) {
      final AST found = findFilterPredicate(expr.getChild(i), maxDepth - 1);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Extract a Path and document context from a deref expression chain.
   * Walks from outer deref inward until reaching a jn:doc() call.
   *
   * <p>Since we walk outside-in but PathSummary stores paths root-to-leaf,
   * we collect steps into a list and reverse them before building the Path.</p>
   */
  private PathAndDocument extractPathAndDocument(AST expr) {
    // Collect path steps outside-in (will be reversed for root-to-leaf order)
    final var steps = new ArrayList<PathStep>(8);
    AST current = expr;
    int maxIterations = 50; // guard against pathological ASTs
    activeVarResolutions = new HashSet<>(4);

    while (current != null && --maxIterations > 0) {
      if (current.getType() == XQ.DerefExpr && current.getChildCount() >= 2) {
        final String fieldName = current.getChild(1).getStringValue();
        if (fieldName != null) {
          steps.add(new PathStep(StepKind.OBJECT_FIELD, fieldName));
        }
        current = current.getChild(0);
      } else if (current.getType() == XQ.ArrayAccess) {
        // Array unboxing — record as array step
        steps.add(new PathStep(StepKind.ARRAY, null));
        current = current.getChild(0);
      } else if (current.getType() == XQ.FunctionCall && isDocFunction(current)) {
        // Reached jn:doc() — build path in root-to-leaf order and extract doc context
        return buildPathAndDocument(current, steps);
      } else if (current.getType() == XQ.FilterExpr && current.getChildCount() >= 1) {
        // Unwrap filter predicate — extract path from the filtered expression.
        // E.g., jn:doc(...)[].item[?$$.price gt 9990] → extract path from jn:doc(...)[].item
        current = current.getChild(0);
      } else if (current.getType() == XQ.VariableRef) {
        // Resolve variable binding — follow the reference to its definition
        final Object varKey = current.getValue();
        if (varKey == null) {
          return null;
        }
        // Cycle detection: if we've already started resolving this variable,
        // we have a circular reference (e.g., let $x := $y, let $y := $x)
        if (!activeVarResolutions.add(varKey)) {
          return null; // cycle detected
        }
        final AST resolvedExpr = variableBindings.get(varKey);
        if (resolvedExpr != null) {
          current = resolvedExpr;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    return null;
  }

  private PathAndDocument buildPathAndDocument(AST docCall, ArrayList<PathStep> steps) {
    if (docCall.getChildCount() < 2) {
      return null;
    }

    final String databaseName = docCall.getChild(0).getStringValue();
    final String resourceName = docCall.getChild(1).getStringValue();

    final int revision;
    if (docCall.getChildCount() > 2) {
      final var revValue = docCall.getChild(2).getValue();
      if (revValue instanceof Number numVal) {
        revision = numVal.intValue();
      } else {
        revision = -1;
      }
    } else {
      revision = -1;
    }

    if (databaseName == null || resourceName == null) {
      return null;
    }

    // Build path in root-to-leaf order (reverse of outside-in walk)
    final var path = new Path<QNm>();
    for (int i = steps.size() - 1; i >= 0; i--) {
      final PathStep step = steps.get(i);
      switch (step.kind) {
        case ARRAY -> path.childArray();
        case OBJECT_FIELD -> path.childObjectField(new QNm(step.name));
      }
    }

    return new PathAndDocument(path, databaseName, resourceName, revision);
  }

  private static boolean isDocFunction(AST node) {
    final var value = node.getValue();
    if (value instanceof QNm qnm) {
      return (JSONFun.JSON_NSURI.equals(qnm.getNamespaceURI())
          && ("doc".equals(qnm.getLocalName()) || "open".equals(qnm.getLocalName())));
    }
    return false;
  }

  /**
   * Schedule deferred histogram collection for a field on cache miss.
   * Registers the field for collection after the current optimization pass
   * completes, avoiding deadlocks from opening resource sessions during
   * an active write transaction.
   *
   * <p>The collected histogram will be available for the next query
   * against this field. The current query uses default selectivity estimates.</p>
   */
  private void triggerAsyncHistogramCollection(String databaseName, String resourceName,
                                                String fieldPath) {
    final String key = databaseName + "/" + resourceName + "/" + fieldPath;
    if (!pendingCollections.add(key)) {
      return; // already scheduled in this optimization pass
    }
    // Collection is deferred: the field is tracked in pendingCollections.
    // Callers can invoke collectPendingHistograms() after optimization
    // completes and the write transaction is closed.
  }

  /**
   * Collect histograms for all fields that had cache misses during the
   * last optimization pass. Call this after the query has been compiled
   * and any write transactions are closed.
   *
   * <p>This is safe to call from a read-only context. Each collected
   * histogram is registered in the {@link StatisticsCatalog} for
   * subsequent queries.</p>
   */
  public void collectPendingHistograms() {
    if (pendingCollections.isEmpty()) {
      return;
    }
    if (histogramCollector == null) {
      histogramCollector = new HistogramCollector(jsonStore);
    }
    for (final String key : pendingCollections) {
      final String[] parts = key.split("/", 3);
      if (parts.length < 3) continue;
      try {
        final boolean collected = histogramCollector.collectAndRegister(
            parts[0], parts[1], parts[2]);
        if (collected) {
          LOG.debug("Deferred histogram collection succeeded for {}", key);
        }
      } catch (Exception e) {
        LOG.debug("Deferred histogram collection failed for {}: {}", key, e.getMessage());
      }
    }
    pendingCollections.clear();
  }

  private enum StepKind { OBJECT_FIELD, ARRAY }

  private record PathStep(StepKind kind, String name) {}

  private record PathAndDocument(Path<QNm> path, String databaseName,
                                 String resourceName, int revision) {}
}
