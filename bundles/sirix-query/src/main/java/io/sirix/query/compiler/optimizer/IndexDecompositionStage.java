package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.walker.json.JoinDecompositionWalker;

/**
 * Index-aware join decomposition stage (Rules 5 and 6).
 *
 * <p>This stage runs after CostBasedStage (which annotates AST nodes with
 * index preference hints) and JqgmRewriteStage (which fuses adjacent joins
 * via Rule 1). It applies:</p>
 * <ul>
 *   <li><b>Rule 5</b>: Swaps join children so the indexed side drives
 *       the join (child 0 = probe/driving side)</li>
 *   <li><b>Rule 6</b>: Creates an intersection join when both sides
 *       have different indexes — both sides keep {@code PREFER_INDEX=true}</li>
 * </ul>
 *
 * <p>Two-phase approach: Phase 1 walks the AST to annotate decomposition
 * metadata (via {@link JoinDecompositionWalker}). Phase 2 reads those
 * annotations and restructures the AST. This avoids mutating the tree
 * during the walker traversal.</p>
 *
 * <p>Based on Weiner et al. Section 4.3, adapted for JSON/JSONiq.</p>
 */
public final class IndexDecompositionStage implements Stage {

  private static final int MAX_SEARCH_DEPTH = 5;

  // Reuse walker instance across calls (prepare() resets state)
  private final JoinDecompositionWalker walker = new JoinDecompositionWalker();

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    // Phase 1: Annotation pass (reads index annotations, marks joins)
    ast = walker.walk(ast);

    // Phase 2: Restructuring pass (reads decomposition annotations, mutates AST)
    if (walker.wasModified()) {
      restructureAnnotatedJoins(ast);
    }
    return ast;
  }

  /**
   * Post-order walk: restructure joins annotated with decomposition metadata.
   * Post-order ensures nested joins are processed before their parents.
   */
  private static void restructureAnnotatedJoins(AST node) {
    if (node == null) {
      return;
    }

    // Process children first (post-order)
    for (int i = 0; i < node.getChildCount(); i++) {
      restructureAnnotatedJoins(node.getChild(i));
    }

    // Only process Join nodes with decomposition annotations
    if (node.getType() != XQ.Join) {
      return;
    }
    if (!Boolean.TRUE.equals(node.getProperty(CostProperties.DECOMPOSITION_APPLICABLE))) {
      return;
    }
    if (Boolean.TRUE.equals(node.getProperty(CostProperties.DECOMPOSITION_RESTRUCTURED))) {
      return; // Already restructured — idempotency guard
    }

    final String type = (String) node.getProperty(CostProperties.DECOMPOSITION_TYPE);
    if (CostProperties.DECOMPOSITION_RULE_5.equals(type)) {
      applyRule5(node);
    } else if (CostProperties.DECOMPOSITION_RULE_6.equals(type)) {
      applyRule6(node);
    }
  }

  /**
   * Rule 5: Swap join children so the indexed side is child 0 (driving side).
   *
   * <p>The indexed side should drive the join because the index scan
   * produces a smaller intermediate result, reducing the number of
   * probe lookups on the non-indexed side.</p>
   */
  private static void applyRule5(AST join) {
    if (join.getChildCount() < 2) {
      return;
    }

    final Object decompositionIndexId = join.getProperty(CostProperties.DECOMPOSITION_INDEX_ID);
    if (!(decompositionIndexId instanceof Integer targetIndexId)) {
      return;
    }

    // Determine which side has the index
    final boolean leftHasIndex = hasIndexAnnotation(join.getChild(0), targetIndexId, MAX_SEARCH_DEPTH);

    if (!leftHasIndex) {
      // Index is on the right — swap so indexed side drives
      final AST leftInput = join.getChild(0);
      final AST rightInput = join.getChild(1);
      join.replaceChild(0, rightInput);
      join.replaceChild(1, leftInput);
    }

    // Ensure PREFER_INDEX=true on the indexed subtree (child 0 after potential swap)
    propagatePreferIndex(join.getChild(0), MAX_SEARCH_DEPTH);

    join.setProperty(CostProperties.DECOMPOSITION_RESTRUCTURED, true);
  }

  /**
   * Rule 6: Create an intersection join when both sides have different indexes.
   *
   * <p>Both input subtrees are index-scannable. We ensure both sides
   * have {@code PREFER_INDEX=true} so that downstream index matching
   * rewrites both to index scans. The join then becomes an intersection
   * of two index result sets.</p>
   */
  private static void applyRule6(AST join) {
    if (join.getChildCount() < 2) {
      return;
    }

    // Mark as intersection join — runtime can use nodeKey equality
    join.setProperty(CostProperties.INTERSECTION_JOIN, true);

    // Ensure both sides have PREFER_INDEX=true for downstream index matching
    propagatePreferIndex(join.getChild(0), MAX_SEARCH_DEPTH);
    propagatePreferIndex(join.getChild(1), MAX_SEARCH_DEPTH);

    join.setProperty(CostProperties.DECOMPOSITION_RESTRUCTURED, true);
  }

  /**
   * Check if a subtree contains a node with the specified INDEX_ID
   * and PREFER_INDEX=true.
   */
  private static boolean hasIndexAnnotation(AST node, int targetIndexId, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return false;
    }

    if (Boolean.TRUE.equals(node.getProperty(CostProperties.PREFER_INDEX))) {
      final Object idProp = node.getProperty(CostProperties.INDEX_ID);
      if (idProp instanceof Integer indexId && indexId == targetIndexId) {
        return true;
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasIndexAnnotation(node.getChild(i), targetIndexId, maxDepth - 1)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Set PREFER_INDEX=true on the first node in the subtree that has
   * index annotations (INDEX_ID set), ensuring CostDrivenRoutingStage
   * keeps the index gate open for this subtree.
   */
  private static void propagatePreferIndex(AST node, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return;
    }

    if (node.getProperty(CostProperties.INDEX_ID) != null) {
      node.setProperty(CostProperties.PREFER_INDEX, true);
      return;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      propagatePreferIndex(node.getChild(i), maxDepth - 1);
    }
  }
}
