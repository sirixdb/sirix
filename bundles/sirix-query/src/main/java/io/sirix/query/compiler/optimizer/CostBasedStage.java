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
import io.sirix.query.json.JsonDBStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

  private final JsonDBStore jsonStore;
  private final JsonCostModel costModel;
  private final SelectivityEstimator selectivityEstimator;
  private final CardinalityEstimator cardinalityEstimator;
  private SirixStatisticsProvider statsProvider;

  /**
   * Tracks variable bindings for resolving VariableRef nodes.
   * Maps the variable's binding key (AST child(0) value) to its binding expression (child(1)).
   * Cleared between queries via rewrite().
   */
  private final Map<Object, AST> variableBindings = new HashMap<>(8);

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
      getStatsProvider();
    } else {
      statsProvider.clearCaches();
    }
    variableBindings.clear();

    annotateSubtree(ast);

    // Phase 2: Annotate cardinality estimates throughout the pipeline
    cardinalityEstimator.annotateCardinalities(ast);

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
      final Histogram histogram = StatisticsCatalog.getInstance()
          .get(databaseName, resourceName, leafField);
      selectivityEstimator.setHistogram(histogram); // null clears previous
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
        final AST resolvedExpr = varKey != null ? variableBindings.get(varKey) : null;
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

  private enum StepKind { OBJECT_FIELD, ARRAY }

  private record PathStep(StepKind kind, String name) {}

  private record PathAndDocument(Path<QNm> path, String databaseName,
                                 String resourceName, int revision) {}
}
