package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.brackit.query.util.Cmp;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Rule 4: Join Commutativity — swaps join inputs when the cost model
 * indicates it is cheaper.
 *
 * <p>For hash joins, the build side (right input) should be the smaller
 * relation. This walker checks if the left input has a lower estimated
 * cardinality than the right input, and if so, swaps them and adjusts
 * the comparison operator.</p>
 *
 * <p>This walker reads {@link CostProperties#ESTIMATED_CARDINALITY}
 * annotations set by the CardinalityEstimator. If annotations are missing,
 * the walker uses {@link CostProperties#PATH_CARDINALITY} as fallback.</p>
 *
 * <p>Left outer joins are NOT reordered (commutativity does not hold).</p>
 */
public final class JoinCommutativityWalker extends Walker {

  private boolean modified;

  public JoinCommutativityWalker() {
    super();
  }

  public boolean wasModified() {
    return modified;
  }

  @Override
  protected AST prepare(AST ast) {
    modified = false;
    return ast;
  }

  @Override
  protected AST visit(AST node) {
    if (node.getType() != XQ.Join) {
      return node;
    }

    // Never reorder left outer joins
    if (CostProperties.isLeftJoin(node)) {
      return node;
    }

    // Already reordered in this pass
    if (node.getProperty(CostProperties.JOIN_SWAPPED) != null) {
      return node;
    }

    // Join has 4 children: [leftInput, rightInput, postPipeline, output]
    if (node.getChildCount() < 4) {
      return node;
    }

    final AST leftInput = node.getChild(0);
    final AST rightInput = node.getChild(1);

    final long leftCard = extractCardinality(leftInput);
    final long rightCard = extractCardinality(rightInput);

    // Skip if cardinalities are unknown
    if (leftCard <= 0 || rightCard <= 0) {
      return node;
    }

    // For hash join: build hash table on smaller side (right input).
    // If left is smaller, swap to make it the right (build) side.
    if (leftCard < rightCard) {
      // Swap left and right inputs
      node.replaceChild(0, rightInput);
      node.replaceChild(1, leftInput);

      // Swap comparison operator (e.g., LT becomes GT)
      swapComparison(node);

      node.setProperty(CostProperties.JOIN_SWAPPED, true);
      node.setProperty(CostProperties.JOIN_LEFT_CARD, rightCard);
      node.setProperty(CostProperties.JOIN_RIGHT_CARD, leftCard);

      modified = true;
    } else {
      // Annotate even if not swapped (for cost tracking)
      node.setProperty(CostProperties.JOIN_LEFT_CARD, leftCard);
      node.setProperty(CostProperties.JOIN_RIGHT_CARD, rightCard);
    }

    return node;
  }

  /**
   * Extract cardinality from an AST node or its shallow descendants.
   * Delegates to {@link CostProperties#extractCardinality(AST, int)}.
   *
   * @return positive cardinality, or -1 if unknown
   */
  private static long extractCardinality(AST node) {
    return CostProperties.extractCardinality(node, 3);
  }

  /**
   * Swap the comparison operator when join inputs are swapped.
   * EQ stays EQ, but LT/GT and LE/GE are reversed.
   */
  private static void swapComparison(AST joinNode) {
    final Object cmpObj = joinNode.getProperty(CostProperties.CMP);
    if (cmpObj instanceof Cmp cmp) {
      joinNode.setProperty(CostProperties.CMP, cmp.swap());
    }
  }
}
