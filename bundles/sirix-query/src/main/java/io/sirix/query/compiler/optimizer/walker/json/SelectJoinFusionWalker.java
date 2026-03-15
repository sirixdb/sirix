package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Rule 2: Select-Join Fusion — pushes Selection predicates into adjacent
 * Join nodes when the predicate only references variables from one side
 * of the join.
 *
 * <p>Before:
 * <pre>
 *   Selection(predicate)
 *     └── Join(cmp)
 *           ├── left input
 *           ├── right input
 *           ├── post pipeline
 *           └── output
 * </pre>
 *
 * After: the Selection predicate is annotated on the Join node for
 * downstream cost-based stages to consider during join ordering.
 *
 * <p>This walker does not restructure the AST — it annotates the Join
 * with metadata indicating a pushed predicate is available. The
 * CostBasedJoinReorder walker uses this information when computing
 * base relation cardinalities (filtered by pushed predicates).</p>
 *
 * <p><b>Current status:</b> Annotation-only. The {@code JOIN_PREDICATE_PUSHED}
 * and {@code FUSED_HAS_PUSHDOWN} annotations are set on Join/Selection nodes,
 * but the physical predicate pushdown into join operators is not yet
 * implemented. The annotations serve as metadata for the cost model
 * (selectivity-aware join reordering) rather than triggering AST restructuring.</p>
 */
public final class SelectJoinFusionWalker extends Walker {

  private boolean modified;

  public SelectJoinFusionWalker() {
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
    if (node.getType() != XQ.Selection) {
      return node;
    }

    // Selection must have exactly 2 children: predicate + pipeline input
    if (node.getChildCount() < 2) {
      return node;
    }

    final AST pipelineInput = node.getChild(node.getChildCount() - 1);

    // Check if the pipeline input is a Join
    if (pipelineInput.getType() != XQ.Join) {
      return node;
    }

    // Don't re-fuse already fused selections
    if (pipelineInput.getProperty(CostProperties.JOIN_PREDICATE_PUSHED) != null) {
      return node;
    }

    final AST predicate = node.getChild(0);

    // Annotate the Join with the pushed predicate info
    pipelineInput.setProperty(CostProperties.JOIN_PREDICATE_PUSHED, true);

    // Annotate the Selection to indicate it was fused
    node.setProperty(CostProperties.JOIN_PREDICATE_PUSHED, true);

    // Annotate selectivity if the predicate is a comparison
    if (predicate.getType() == XQ.ComparisonExpr
        || predicate.getType() == XQ.AndExpr
        || predicate.getType() == XQ.OrExpr) {
      pipelineInput.setProperty(CostProperties.FUSED_HAS_PUSHDOWN, true);
    }

    modified = true;
    return node;
  }
}
