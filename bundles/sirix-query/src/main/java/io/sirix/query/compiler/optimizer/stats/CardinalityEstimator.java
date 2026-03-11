package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;

/**
 * Propagates cardinality estimates through the FLWOR pipeline AST.
 *
 * <p>Walks the pipeline (ForBind → LetBind → Selection → GroupBy → OrderBy)
 * and computes estimated output cardinality at each stage using
 * statistics from {@link StatisticsProvider} and selectivities
 * from {@link SelectivityEstimator}.</p>
 *
 * <p>Key propagation rules:
 * <ul>
 *   <li>ForBind: {@code card(R) × card(binding)} — each binding can produce multiple tuples</li>
 *   <li>LetBind: preserves input cardinality (single binding per tuple)</li>
 *   <li>Selection: {@code card(input) × selectivity(predicate)}</li>
 *   <li>GroupBy: {@code √(card(input))} — standard heuristic when no histogram available</li>
 *   <li>OrderBy: preserves cardinality (reorder only)</li>
 * </ul></p>
 */
public final class CardinalityEstimator {

  private static final int MAX_PIPELINE_DEPTH = 500;
  private static final long DEFAULT_BINDING_CARDINALITY = 10L;

  private final SelectivityEstimator selectivityEstimator;

  public CardinalityEstimator(SelectivityEstimator selectivityEstimator) {
    this.selectivityEstimator = selectivityEstimator;
  }

  /**
   * Estimate the output cardinality of a FLWOR pipeline rooted at the given AST node.
   *
   * @param pipelineNode the root of the pipeline (typically a FlowrExpr or ForBind)
   * @return estimated output cardinality (always ≥ 1)
   */
  public long estimatePipelineCardinality(AST pipelineNode) {
    return walkPipeline(pipelineNode, 0);
  }

  /**
   * Annotate an AST subtree with estimated cardinalities on ForBind, LetBind,
   * and Selection nodes. Sets the property {@code "costBased.estimatedCardinality"}.
   *
   * @param node the root of the subtree to annotate
   */
  public void annotateCardinalities(AST node) {
    if (node == null) {
      return;
    }
    if (node.getType() == XQ.ForBind || node.getType() == XQ.LetBind
        || node.getType() == XQ.Selection || node.getType() == XQ.FilterExpr) {
      final long cardinality = walkPipeline(node, 0);
      node.setProperty("costBased.estimatedCardinality", cardinality);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      annotateCardinalities(node.getChild(i));
    }
  }

  private long walkPipeline(AST node, int depth) {
    if (node == null || depth > MAX_PIPELINE_DEPTH) {
      return 1L;
    }

    final int type = node.getType();

    if (type == XQ.ForBind) {
      // card(for $x in R return f($x)) = card(R) × card(binding)
      final long bindingCard = estimateBindingCardinality(node);
      final long inputCard = walkChildPipeline(node, depth);
      return Math.max(1L, inputCard * bindingCard);
    }

    if (type == XQ.LetBind) {
      // LetBind does NOT multiply cardinality
      return Math.max(1L, walkChildPipeline(node, depth));
    }

    if (type == XQ.Selection) {
      // card(σ_p(R)) = card(input) × selectivity(p)
      final long inputCard = walkChildPipeline(node, depth);
      final double selectivity = estimateSelectionSelectivity(node);
      return Math.max(1L, (long) (inputCard * selectivity));
    }

    if (type == XQ.FilterExpr) {
      // FilterExpr: source[predicate] — apply predicate selectivity
      final long sourceCard = walkPipeline(node.getChild(0), depth + 1);
      if (node.getChildCount() >= 2 && node.getChild(1).getType() == XQ.Predicate) {
        final double selectivity = selectivityEstimator.estimateSelectivity(node.getChild(1));
        return Math.max(1L, (long) (sourceCard * selectivity));
      }
      return sourceCard;
    }

    if (type == XQ.GroupBy) {
      // Heuristic: √(input cardinality)
      final long inputCard = walkChildPipeline(node, depth);
      return Math.max(1L, (long) Math.sqrt(inputCard));
    }

    if (type == XQ.OrderBy) {
      // OrderBy preserves cardinality
      return walkChildPipeline(node, depth);
    }

    if (type == XQ.Join) {
      // Join: card(R) × card(S) × joinSelectivity
      if (node.getChildCount() >= 2) {
        final long leftCard = walkPipeline(node.getChild(0), depth + 1);
        final long rightCard = walkPipeline(node.getChild(1), depth + 1);
        // Default join selectivity: reciprocal of the larger relation
        final double joinSel = 1.0 / Math.max(leftCard, rightCard);
        return Math.max(1L, (long) (leftCard * rightCard * joinSel));
      }
    }

    // Default: try to recurse into last child (pipeline continuation)
    return walkChildPipeline(node, depth);
  }

  private long walkChildPipeline(AST node, int depth) {
    if (node.getChildCount() > 0) {
      return walkPipeline(node.getChild(node.getChildCount() - 1), depth + 1);
    }
    return 1L;
  }

  /**
   * Estimate the cardinality of a ForBind's binding expression.
   * Uses costBased.pathCardinality if available (from M1 CostBasedStage).
   */
  private long estimateBindingCardinality(AST forBind) {
    if (forBind.getChildCount() < 2) {
      return DEFAULT_BINDING_CARDINALITY;
    }
    final AST bindingExpr = forBind.getChild(1);

    // Check if CostBasedStage (M1) already annotated this
    final Object pathCard = bindingExpr.getProperty("costBased.pathCardinality");
    if (pathCard instanceof Number numCard) {
      return Math.max(1L, numCard.longValue());
    }

    return DEFAULT_BINDING_CARDINALITY;
  }

  /**
   * Estimate selectivity for a Selection node.
   * Selection typically has predicate as child(0).
   */
  private double estimateSelectionSelectivity(AST selectionNode) {
    if (selectionNode.getChildCount() > 0) {
      return selectivityEstimator.estimateSelectivity(selectionNode.getChild(0));
    }
    return 0.5;
  }
}
