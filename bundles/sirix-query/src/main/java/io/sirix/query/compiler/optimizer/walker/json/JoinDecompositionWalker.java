package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Rules 5-6: Index-aware join decomposition.
 *
 * <p><b>Rule 5 (Complex Join Decomposition)</b>: When an n-way join group
 * (detected by {@link JoinFusionWalker}) has a subtree that maps to an
 * available index, this walker annotates the join to indicate that the
 * subtree should be split at the index boundary.</p>
 *
 * <p><b>Rule 6 (Overlapping Index Decomposition)</b>: When multiple indexes
 * overlap at the JPP root (i.e., two child subtrees of a join root are each
 * covered by a different index), this walker annotates the join to indicate
 * that a structural self-join (intersection) is needed.</p>
 *
 * <p>This walker operates on AST annotations rather than restructuring the
 * tree. It reads index-related annotations set by {@code CostBasedStage}
 * and fusion annotations set by {@code JoinFusionWalker}, then marks joins
 * with decomposition metadata for the {@code IndexDecompositionStage}.</p>
 *
 * <p>Based on Weiner et al. Rules 5-6 (Section 4.3).</p>
 */
public final class JoinDecompositionWalker extends Walker {

  private static final int MAX_SEARCH_DEPTH = 5;

  private boolean modified;

  public JoinDecompositionWalker() {
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

    // Skip left outer joins
    if (CostProperties.isLeftJoin(node)) {
      return node;
    }

    // Skip already decomposed joins
    if (node.getProperty(CostProperties.DECOMPOSITION_APPLICABLE) != null) {
      return node;
    }

    // Need at least 2 children (left + right inputs)
    if (node.getChildCount() < 2) {
      return node;
    }

    // Compute index annotations once for both rules
    final IndexAnnotation leftIndex = findIndexAnnotation(node.getChild(0), MAX_SEARCH_DEPTH);
    final IndexAnnotation rightIndex = findIndexAnnotation(node.getChild(1), MAX_SEARCH_DEPTH);

    // Rule 5: exactly one side has an index
    if (leftIndex != null && rightIndex == null) {
      annotateDecomposition(node, CostProperties.DECOMPOSITION_RULE_5, leftIndex);
      modified = true;
      return node;
    }
    if (rightIndex != null && leftIndex == null) {
      annotateDecomposition(node, CostProperties.DECOMPOSITION_RULE_5, rightIndex);
      modified = true;
      return node;
    }

    // Rule 6: both sides have different indexes — intersection
    if (leftIndex != null && rightIndex != null
        && leftIndex.indexId != rightIndex.indexId) {
      annotateIntersectionDecomposition(node, leftIndex, rightIndex);
      modified = true;
      return node;
    }

    return node;
  }

  /**
   * Search an input subtree for index preference annotations set by CostBasedStage.
   * Depth-limited to avoid deep traversal on large ASTs.
   */
  private static IndexAnnotation findIndexAnnotation(AST node, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return null;
    }

    if (Boolean.TRUE.equals(node.getProperty(CostProperties.PREFER_INDEX))) {
      final Object idProp = node.getProperty(CostProperties.INDEX_ID);
      final Object typeProp = node.getProperty(CostProperties.INDEX_TYPE);
      if (idProp instanceof Integer indexId && typeProp instanceof String indexType) {
        return new IndexAnnotation(indexId, indexType);
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      final IndexAnnotation found = findIndexAnnotation(node.getChild(i), maxDepth - 1);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Annotate a join node with decomposition metadata.
   */
  private static void annotateDecomposition(AST join, String type,
                                            IndexAnnotation index) {
    join.setProperty(CostProperties.DECOMPOSITION_APPLICABLE, true);
    join.setProperty(CostProperties.DECOMPOSITION_TYPE, type);
    join.setProperty(CostProperties.DECOMPOSITION_INDEX_ID, index.indexId);
    join.setProperty(CostProperties.DECOMPOSITION_INDEX_TYPE, index.indexType);
  }

  /**
   * Annotate a join node with Rule 6 intersection decomposition metadata.
   */
  private static void annotateIntersectionDecomposition(AST join,
                                                        IndexAnnotation leftIndex,
                                                        IndexAnnotation rightIndex) {
    annotateDecomposition(join, CostProperties.DECOMPOSITION_RULE_6, leftIndex);
    join.setProperty(CostProperties.DECOMPOSITION_INTERSECT, true);
    join.setProperty(CostProperties.DECOMPOSITION_INDEX_ID_RIGHT, rightIndex.indexId);
    join.setProperty(CostProperties.DECOMPOSITION_INDEX_TYPE_RIGHT, rightIndex.indexType);
  }

  /**
   * Lightweight holder for index annotation data found in AST subtrees.
   */
  private record IndexAnnotation(int indexId, String indexType) {}
}
