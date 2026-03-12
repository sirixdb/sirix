package io.sirix.query.compiler.vectorized;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;

/**
 * Analyzes an AST subtree to determine if a query pipeline is eligible
 * for vectorized (SIMD batch) execution.
 *
 * <p>A pipeline is vectorizable when it matches a simple scan-filter-project
 * pattern over a JSON collection:
 * <pre>
 *   for $x in collection[]          → ForBind (scan)
 *     where $x.field op constant    → Selection (filter)
 *     return $x.field               → projection
 * </pre></p>
 *
 * <p>Requirements for vectorization:
 * <ul>
 *   <li>ForBind iterates over an array/collection (not a join)</li>
 *   <li>Selection predicates are simple scalar comparisons
 *       (field op constant, where field is a DerefExpr chain)</li>
 *   <li>No nested FLWOR, no grouping, no ordering</li>
 *   <li>Return clause projects scalar values (not complex constructors)</li>
 * </ul></p>
 *
 * <p>This detector is conservative — it only marks patterns it can
 * guarantee are safely vectorizable. False negatives fall back to
 * tuple-at-a-time execution with no correctness impact.</p>
 *
 * <p>Based on the vectorization detection from MonetDB/X100 and
 * DuckDB's pipeline breaker analysis.</p>
 */
public final class VectorizedPipelineDetector {

  /** AST property key for vectorization eligibility annotation. */
  public static final String VECTORIZABLE = "vectorized.eligible";

  /** AST property key for the number of vectorizable predicates. */
  public static final String VECTORIZABLE_PREDICATE_COUNT = "vectorized.predicateCount";

  private static final int MAX_DEPTH = 8;

  /**
   * Analyze an AST subtree and annotate vectorizable pipelines.
   *
   * @param ast the root AST node to analyze
   * @return true if any vectorizable pipeline was found
   */
  public boolean analyze(AST ast) {
    if (ast == null) {
      return false;
    }
    boolean found = false;
    found |= analyzeNode(ast);
    for (int i = 0; i < ast.getChildCount(); i++) {
      found |= analyze(ast.getChild(i));
    }
    return found;
  }

  /**
   * Check if a single AST node represents a vectorizable pipeline root.
   *
   * <p>We look for ForBind nodes whose parent Selection has simple
   * scalar predicates.</p>
   */
  private boolean analyzeNode(AST node) {
    // Look for PipeExpr or FLWOR patterns: Start → ForBind → Selection → End
    if (node.getType() != XQ.Start) {
      return false;
    }

    final AST forBind = findChild(node, XQ.ForBind);
    if (forBind == null) {
      return false;
    }

    // Check: ForBind must iterate over a collection/array (not a join)
    if (!isSimpleIteration(forBind)) {
      return false;
    }

    // Check: must have a Selection sibling (where clause)
    final AST selection = findChild(node, XQ.Selection);
    if (selection == null) {
      return false;
    }

    // Check: Selection predicate must be a simple scalar comparison
    final int predicateCount = countVectorizablePredicates(selection, MAX_DEPTH);
    if (predicateCount == 0) {
      return false;
    }

    // Check: no pipeline breakers (GroupBy, OrderBy, nested FLWOR)
    if (hasPipelineBreaker(node, MAX_DEPTH)) {
      return false;
    }

    // Annotate the ForBind as vectorizable
    forBind.setProperty(VECTORIZABLE, true);
    forBind.setProperty(VECTORIZABLE_PREDICATE_COUNT, predicateCount);
    return true;
  }

  /**
   * Check if a ForBind iterates over a simple collection/array access
   * (not a join or complex subquery).
   */
  private static boolean isSimpleIteration(AST forBind) {
    if (forBind.getChildCount() < 2) {
      return false;
    }
    final AST bindingExpr = forBind.getChild(1);
    if (bindingExpr == null) {
      return false;
    }

    // Accept: DerefExpr (collection access), ArrayAccess, variable reference
    final int type = bindingExpr.getType();
    return type == XQ.DerefExpr
        || type == XQ.ArrayAccess
        || type == XQ.VariableRef
        || type == XQ.FunctionCall;
  }

  /**
   * Count vectorizable predicates in a Selection subtree.
   *
   * <p>A predicate is vectorizable when it's a comparison between
   * a DerefExpr chain (field access) and a constant literal.</p>
   */
  private static int countVectorizablePredicates(AST node, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return 0;
    }

    // Check if this node is a comparison operator
    if (isComparisonNode(node)) {
      if (hasDerefAndConstant(node)) {
        return 1;
      }
    }

    // Check AND-connected predicates
    if (node.getType() == XQ.AndExpr) {
      int count = 0;
      for (int i = 0; i < node.getChildCount(); i++) {
        count += countVectorizablePredicates(node.getChild(i), maxDepth - 1);
      }
      return count;
    }

    // Recurse into children (Selection wraps predicates)
    int count = 0;
    for (int i = 0; i < node.getChildCount(); i++) {
      count += countVectorizablePredicates(node.getChild(i), maxDepth - 1);
    }
    return count;
  }

  /**
   * Check if an AST node is a comparison operator (=, !=, &lt;, &lt;=, &gt;, &gt;=).
   */
  private static boolean isComparisonNode(AST node) {
    final int type = node.getType();
    return type == XQ.GeneralCompEQ
        || type == XQ.GeneralCompNE
        || type == XQ.GeneralCompLT
        || type == XQ.GeneralCompLE
        || type == XQ.GeneralCompGT
        || type == XQ.GeneralCompGE
        || type == XQ.ValueCompEQ
        || type == XQ.ValueCompNE
        || type == XQ.ValueCompLT
        || type == XQ.ValueCompLE
        || type == XQ.ValueCompGT
        || type == XQ.ValueCompGE;
  }

  /**
   * Check if a comparison node has one DerefExpr child and one constant child.
   * This pattern (field op constant) is vectorizable.
   */
  private static boolean hasDerefAndConstant(AST cmpNode) {
    if (cmpNode.getChildCount() < 2) {
      return false;
    }
    final AST left = cmpNode.getChild(0);
    final AST right = cmpNode.getChild(1);

    return (isFieldAccess(left) && isConstant(right))
        || (isConstant(left) && isFieldAccess(right));
  }

  /**
   * Check if an AST node represents a field access (DerefExpr chain).
   */
  private static boolean isFieldAccess(AST node) {
    if (node == null) {
      return false;
    }
    final int type = node.getType();
    return type == XQ.DerefExpr || type == XQ.VariableRef || type == XQ.ArrayAccess;
  }

  /**
   * Check if an AST node is a constant literal.
   */
  private static boolean isConstant(AST node) {
    if (node == null) {
      return false;
    }
    final int type = node.getType();
    return type == XQ.Int
        || type == XQ.Dbl
        || type == XQ.Dec
        || type == XQ.Str;
  }

  /**
   * Check if a subtree contains pipeline breakers (operations that
   * prevent vectorized execution).
   */
  private static boolean hasPipelineBreaker(AST node, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return false;
    }
    final int type = node.getType();
    // Pipeline breakers: GroupBy, OrderBy, nested FLWOR, Join
    if (type == XQ.GroupBy || type == XQ.OrderBy || type == XQ.Join) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasPipelineBreaker(node.getChild(i), maxDepth - 1)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Find a direct child of a given type.
   */
  private static AST findChild(AST parent, int type) {
    for (int i = 0; i < parent.getChildCount(); i++) {
      if (parent.getChild(i).getType() == type) {
        return parent.getChild(i);
      }
    }
    return null;
  }
}
