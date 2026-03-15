package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.vectorized.ColumnType;
import io.sirix.query.compiler.vectorized.ComparisonOperator;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.json.JSONFun;
import io.sirix.query.compiler.vectorized.VectorizedPipelineDetector;
import io.sirix.query.compiler.vectorized.VectorizedPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimization stage that routes vectorizable pipelines to columnar/SIMD execution.
 *
 * <p>Runs after {@link VectorizedDetectionStage}. For each ForBind annotated
 * with {@code VECTORIZABLE=true}, this stage:
 * <ol>
 *   <li>Finds the sibling Selection in the parent Start pipeline</li>
 *   <li>Extracts comparison predicates (field name, operator, constant, type)</li>
 *   <li>Determines the execution route (columnar vs SIMD)</li>
 *   <li>Replaces the Start pipeline subtree with a {@code VectorizedPipelineExpr}
 *       AST node carrying all extracted metadata</li>
 * </ol></p>
 *
 * <p>Route decision: if {@code COLUMNAR_STRING_SCAN_ELIGIBLE=true} → "columnar",
 * otherwise → "simd".</p>
 */
public final class VectorizedRoutingStage implements Stage {

  /** AST property key for the vectorized execution route. */
  public static final String VECTORIZED_ROUTE = "vectorized.route";

  /** AST property key for the extracted predicate list. */
  public static final String VECTORIZED_PREDICATES = "vectorized.predicates";

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    routeVectorizedPipelines(ast);
    return ast;
  }

  /**
   * Walk the AST looking for Start nodes that contain vectorizable ForBind children.
   * Replace qualifying Start subtrees with VectorizedPipelineExpr nodes.
   */
  private static void routeVectorizedPipelines(AST node) {
    if (node == null) {
      return;
    }

    // Process children in reverse order since we may replace them
    for (int i = node.getChildCount() - 1; i >= 0; i--) {
      final AST child = node.getChild(i);
      routeVectorizedPipelines(child);
    }

    // Look for Start nodes containing vectorizable ForBind
    if (node.getType() == XQ.Start) {
      tryRouteStartPipeline(node);
    }
  }

  /**
   * Attempt to route a Start pipeline to vectorized execution.
   */
  private static void tryRouteStartPipeline(AST startNode) {
    final AST forBind = findChild(startNode, XQ.ForBind);
    if (forBind == null) {
      return;
    }

    // Check if ForBind is marked as vectorizable
    if (!Boolean.TRUE.equals(forBind.getProperty(VectorizedPipelineDetector.VECTORIZABLE))) {
      return;
    }

    // Find the Selection sibling
    final AST selection = findChild(startNode, XQ.Selection);
    if (selection == null) {
      return;
    }

    // Extract predicates from the Selection
    final List<VectorizedPredicate> predicates = new ArrayList<>();
    extractPredicates(selection, predicates);
    if (predicates.isEmpty()) {
      return;
    }

    // Determine route
    final boolean columnarEligible = Boolean.TRUE.equals(
        forBind.getProperty(VectorizedPipelineDetector.COLUMNAR_STRING_SCAN_ELIGIBLE));
    final String route = columnarEligible ? "columnar" : "simd";

    // Extract database/resource info from the binding expression
    String databaseName = null;
    String resourceName = null;
    Integer revision = null;
    if (forBind.getChildCount() >= 2) {
      final AST bindingExpr = forBind.getChild(1);
      final DocInfo docInfo = extractDocInfo(bindingExpr);
      if (docInfo != null) {
        databaseName = docInfo.database;
        resourceName = docInfo.resource;
        revision = docInfo.revision;
      }
    }

    // Replace the Start node with a VectorizedPipelineExpr AST node
    final AST vectorizedNode = new AST(XQExt.VectorizedPipelineExpr,
        XQExt.toName(XQExt.VectorizedPipelineExpr));
    vectorizedNode.setProperty(VECTORIZED_ROUTE, route);
    vectorizedNode.setProperty(VECTORIZED_PREDICATES, predicates);
    if (databaseName != null) {
      vectorizedNode.setProperty("databaseName", databaseName);
    }
    if (resourceName != null) {
      vectorizedNode.setProperty("resourceName", resourceName);
    }
    if (revision != null) {
      vectorizedNode.setProperty("revision", revision);
    }

    // Replace the Start node in the parent.
    // A Start node without a parent would be a malformed AST — skip replacement
    // rather than silently discarding the built vectorizedNode.
    final AST parent = startNode.getParent();
    if (parent == null) {
      assert false : "Start node has no parent — AST is malformed, skipping vectorized replacement";
      return;
    }
    for (int i = 0; i < parent.getChildCount(); i++) {
      if (parent.getChild(i) == startNode) {
        parent.replaceChild(i, vectorizedNode);
        break;
      }
    }
  }

  /**
   * Recursively extract comparison predicates from a Selection subtree.
   * Handles AND-connected predicates producing multiple entries.
   */
  static void extractPredicates(AST node, List<VectorizedPredicate> predicates) {
    if (node == null) {
      return;
    }

    if (isComparisonNode(node)) {
      final VectorizedPredicate pred = extractSinglePredicate(node);
      if (pred != null) {
        predicates.add(pred);
      }
      return;
    }

    if (node.getType() == XQ.AndExpr) {
      for (int i = 0; i < node.getChildCount(); i++) {
        extractPredicates(node.getChild(i), predicates);
      }
      return;
    }

    // Recurse into structural wrappers (Selection, etc.)
    for (int i = 0; i < node.getChildCount(); i++) {
      extractPredicates(node.getChild(i), predicates);
    }
  }

  /**
   * Extract a single predicate from a comparison node.
   * Expects form: field op constant (or constant op field).
   *
   * @return the extracted predicate, or null if not extractable
   */
  private static VectorizedPredicate extractSinglePredicate(AST cmpNode) {
    if (cmpNode.getChildCount() < 2) {
      return null;
    }

    final AST left = cmpNode.getChild(0);
    final AST right = cmpNode.getChild(1);

    final ComparisonOperator op = mapOperator(cmpNode.getType());
    if (op == null) {
      return null;
    }

    // Determine which side is the field and which is the constant
    String fieldName;
    Object constant;
    ColumnType type;

    if (isFieldAccess(left) && isConstant(right)) {
      fieldName = extractFieldName(left);
      constant = extractConstantValue(right);
      type = mapColumnType(right);
    } else if (isConstant(left) && isFieldAccess(right)) {
      fieldName = extractFieldName(right);
      constant = extractConstantValue(left);
      type = mapColumnType(left);
    } else {
      return null;
    }

    if (fieldName == null || constant == null || type == null) {
      return null;
    }

    return new VectorizedPredicate(fieldName, op, constant, type);
  }

  /**
   * Map XQ comparison constant to ComparisonOperator.
   */
  static ComparisonOperator mapOperator(int xqType) {
    return switch (xqType) {
      case XQ.GeneralCompEQ, XQ.ValueCompEQ -> ComparisonOperator.EQ;
      case XQ.GeneralCompNE, XQ.ValueCompNE -> ComparisonOperator.NE;
      case XQ.GeneralCompLT, XQ.ValueCompLT -> ComparisonOperator.LT;
      case XQ.GeneralCompLE, XQ.ValueCompLE -> ComparisonOperator.LE;
      case XQ.GeneralCompGT, XQ.ValueCompGT -> ComparisonOperator.GT;
      case XQ.GeneralCompGE, XQ.ValueCompGE -> ComparisonOperator.GE;
      default -> null;
    };
  }

  /**
   * Extract the leaf field name from a DerefExpr chain.
   *
   * <p>For nested paths like {@code $x.address.city}, returns the leaf field
   * ({@code "city"}), which is the field whose value is compared in the predicate.
   * The outermost DerefExpr's last Str child is preferred; for nested DerefExpr
   * chains, recursion finds the innermost leaf.</p>
   */
  static String extractFieldName(AST node) {
    return extractFieldNameImpl(node, 32);
  }

  /** Max depth limit prevents StackOverflowError on pathological AST structures. */
  private static String extractFieldNameImpl(AST node, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return null;
    }

    // For DerefExpr, find the leaf field name (last Str child at the deepest level)
    if (node.getType() == XQ.DerefExpr) {
      for (int i = node.getChildCount() - 1; i >= 0; i--) {
        final AST child = node.getChild(i);
        if (child.getType() == XQ.Str) {
          return child.getStringValue();
        }
        // Recurse into nested DerefExpr to find the leaf
        if (child.getType() == XQ.DerefExpr) {
          return extractFieldNameImpl(child, maxDepth - 1);
        }
      }
      // Try the node's own string value
      if (node.getStringValue() != null) {
        return node.getStringValue();
      }
    }

    // For VariableRef or ArrayAccess, try string value
    if (node.getStringValue() != null) {
      return node.getStringValue();
    }

    // Recurse into children
    for (int i = 0; i < node.getChildCount(); i++) {
      final String name = extractFieldNameImpl(node.getChild(i), maxDepth - 1);
      if (name != null) {
        return name;
      }
    }

    return null;
  }

  /**
   * Extract the constant value from a literal node.
   */
  private static Object extractConstantValue(AST node) {
    if (node == null) {
      return null;
    }
    return switch (node.getType()) {
      case XQ.Int -> {
        try {
          yield Long.parseLong(node.getStringValue());
        } catch (NumberFormatException e) {
          yield null;
        }
      }
      case XQ.Dbl, XQ.Dec -> {
        try {
          yield Double.parseDouble(node.getStringValue());
        } catch (NumberFormatException e) {
          yield null;
        }
      }
      case XQ.Str -> node.getStringValue();
      default -> null;
    };
  }

  /**
   * Map a constant literal node type to a ColumnType.
   */
  private static ColumnType mapColumnType(AST constantNode) {
    return switch (constantNode.getType()) {
      case XQ.Int -> ColumnType.INT64;
      case XQ.Dbl, XQ.Dec -> ColumnType.FLOAT64;
      case XQ.Str -> ColumnType.STRING;
      default -> null;
    };
  }

  /**
   * Check if an AST node is a comparison operator.
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
   * Extract database/resource info from a binding expression's jn:doc() call.
   */
  private static DocInfo extractDocInfo(AST bindingExpr) {
    if (bindingExpr == null) {
      return null;
    }

    // Look for FunctionCall (jn:doc) pattern
    if (bindingExpr.getType() == XQ.FunctionCall) {
      return extractDocInfoFromFunctionCall(bindingExpr);
    }

    // Recurse into children (e.g., ArrayAccess wrapping a FunctionCall)
    for (int i = 0; i < bindingExpr.getChildCount(); i++) {
      final DocInfo info = extractDocInfo(bindingExpr.getChild(i));
      if (info != null) {
        return info;
      }
    }

    return null;
  }

  /**
   * Extract database and resource names from a jn:doc() function call.
   * Expected pattern: FunctionCall("jn:doc") with Str children for db and resource.
   */
  private static DocInfo extractDocInfoFromFunctionCall(AST funcCall) {
    // Use proper QNm-based matching (consistent with CostBasedStage.isDocFunction)
    final var value = funcCall.getValue();
    if (value instanceof QNm qnm) {
      if (!JSONFun.JSON_NSURI.equals(qnm.getNamespaceURI())
          || (!"doc".equals(qnm.getLocalName()) && !"open".equals(qnm.getLocalName())
              && !"collection".equals(qnm.getLocalName()))) {
        return null;
      }
    } else {
      return null;
    }

    String database = null;
    String resource = null;
    Integer revision = null;

    for (int i = 0; i < funcCall.getChildCount(); i++) {
      final AST arg = funcCall.getChild(i);
      if (arg.getType() == XQ.Str && arg.getStringValue() != null) {
        if (database == null) {
          database = arg.getStringValue();
        } else if (resource == null) {
          resource = arg.getStringValue();
        }
      } else if (arg.getType() == XQ.Int && arg.getStringValue() != null) {
        try {
          revision = Integer.parseInt(arg.getStringValue());
        } catch (NumberFormatException ignored) {
          // skip
        }
      }
    }

    if (database != null) {
      return new DocInfo(database, resource, revision);
    }
    return null;
  }

  /**
   * Find a direct child of a given type.
   */
  private static AST findChild(AST parent, int type) {
    for (int i = 0; i < parent.getChildCount(); i++) {
      final AST child = parent.getChild(i);
      if (child.getType() == type) {
        return child;
      }
    }
    return null;
  }

  /**
   * Database/resource info extracted from jn:doc() calls.
   */
  private record DocInfo(String database, String resource, Integer revision) {}
}
