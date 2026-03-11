package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule 1: Join Fusion — fuses adjacent binary joins that evaluate JSON deref
 * chains (child-key or descendant-key navigation) into a logically n-way join.
 *
 * <p>Before (two binary joins):
 * <pre>
 *   Join₂(cmp₂)
 *     ├── ForBind(v₂) → DerefExpr chain
 *     ├── Join₁(cmp₁)
 *     │     ├── ForBind(v₁) → DerefExpr chain
 *     │     └── base input
 *     └── ...
 * </pre>
 *
 * After: both joins are annotated as fusable with a shared group ID,
 * enabling downstream stages (IndexDecompositionStage) to treat them
 * as a single n-way join for index decomposition.
 *
 * <p>This walker annotates rather than restructures — it marks join groups
 * with {@link CostProperties#JOIN_FUSED} and a shared
 * {@link CostProperties#JOIN_FUSION_GROUP_ID} for downstream processing.</p>
 *
 * <p>Based on Weiner et al. Rule 1 (Section 4.3).</p>
 */
public final class JoinFusionWalker extends Walker {

  private static final int MAX_SEARCH_DEPTH = 10;
  private static final int MAX_CHAIN_DEPTH = 16;

  private boolean modified;
  private int nextGroupId;

  public JoinFusionWalker() {
    super();
  }

  public boolean wasModified() {
    return modified;
  }

  @Override
  protected AST prepare(AST ast) {
    modified = false;
    nextGroupId = 0;
    return ast;
  }

  @Override
  protected AST visit(AST node) {
    if (node.getType() != XQ.Join) {
      return node;
    }

    // Skip left outer joins — fusion changes semantics
    if (CostProperties.isLeftJoin(node)) {
      return node;
    }

    // Skip already fused joins
    if (node.getProperty(CostProperties.JOIN_FUSED) != null) {
      return node;
    }

    // Quick check: if no nested join via deref, no chain is possible — skip allocation
    if (findNestedJoinViaDeref(node) == null) {
      return node;
    }

    // Collect the fusable chain and annotate if >= 2
    final List<AST> chain = new ArrayList<>(4);
    collectFusableChain(node, chain, MAX_CHAIN_DEPTH);

    if (chain.size() < 2) {
      return node;
    }

    // Annotate all joins in the chain with shared group ID
    final int groupId = nextGroupId++;
    final int stepCount = chain.size();
    for (final AST join : chain) {
      join.setProperty(CostProperties.JOIN_FUSED, true);
      join.setProperty(CostProperties.JOIN_FUSION_GROUP_ID, groupId);
      join.setProperty(CostProperties.JOIN_FUSION_STEP_COUNT, stepCount);
    }

    modified = true;
    return node;
  }

  /**
   * Collect the fusable join chain rooted at this node in a single pass.
   * A chain is fusable when joins are connected by ForBind over DerefExpr
   * chains (structural predicates only). Chain depth is bounded to prevent
   * stack overflow on pathological inputs.
   */
  private static void collectFusableChain(AST join, List<AST> chain, int maxChainDepth) {
    if (maxChainDepth <= 0) {
      return;
    }
    if (join.getType() != XQ.Join || CostProperties.isLeftJoin(join)) {
      return;
    }
    if (join.getProperty(CostProperties.JOIN_FUSED) != null) {
      return;
    }

    chain.add(join);

    final AST nestedJoin = findNestedJoinViaDeref(join);
    if (nestedJoin != null) {
      collectFusableChain(nestedJoin, chain, maxChainDepth - 1);
    }
  }

  /**
   * Find a nested Join in this join's inputs that is connected via
   * a ForBind → DerefExpr chain (structural navigation predicate).
   * Checks both join presence and structural connection in the same
   * input subtree (structural evidence may come from a sibling node,
   * e.g., a ForBind sibling of the nested Join under a Start wrapper).
   */
  private static AST findNestedJoinViaDeref(AST join) {
    final int inputCount = Math.min(join.getChildCount(), 2);
    for (int i = 0; i < inputCount; i++) {
      final AST input = join.getChild(i);
      final AST nested = findJoinInInput(input, MAX_SEARCH_DEPTH);
      if (nested != null && hasStructuralConnection(input, MAX_SEARCH_DEPTH)) {
        return nested;
      }
    }
    return null;
  }

  /**
   * Navigate through Start/ForBind wrappers to find a nested Join node.
   * Depth-limited to avoid deep recursion on pathological inputs.
   */
  private static AST findJoinInInput(AST node, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return null;
    }
    if (node.getType() == XQ.Join && !CostProperties.isLeftJoin(node)) {
      return node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final AST child = node.getChild(i);
      if (child.getType() == XQ.Join && !CostProperties.isLeftJoin(child)) {
        return child;
      }
      if (child.getType() == XQ.Start || child.getType() == XQ.ForBind
          || child.getType() == XQ.LetBind) {
        final AST found = findJoinInInput(child, maxDepth - 1);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /**
   * Check if an input subtree contains a structural (DerefExpr/ArrayAccess) connection.
   * Rule 1 only fuses joins connected by child-key or descendant-key navigation.
   * Depth-limited to avoid deep recursion.
   */
  private static boolean hasStructuralConnection(AST input, int maxDepth) {
    if (input == null || maxDepth <= 0) {
      return false;
    }
    if (input.getType() == XQ.DerefExpr || input.getType() == XQ.ArrayAccess) {
      return true;
    }
    // ForBind: check binding expression (child 1) directly
    if (input.getType() == XQ.ForBind && input.getChildCount() >= 2) {
      return hasStructuralConnection(input.getChild(1), maxDepth - 1);
    }
    for (int i = 0; i < input.getChildCount(); i++) {
      if (hasStructuralConnection(input.getChild(i), maxDepth - 1)) {
        return true;
      }
    }
    return false;
  }
}
