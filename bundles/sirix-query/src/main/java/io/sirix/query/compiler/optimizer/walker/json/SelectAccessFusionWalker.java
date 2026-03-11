package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Rule 3: Select-Access Fusion — pushes value predicates from FilterExpr
 * into the access expression, enabling direct mapping to CAS/PATH indexes.
 *
 * <p>Before fusion (two separate AST nodes):
 * <pre>
 *   FilterExpr
 *     ├── ArrayAccess(DerefExpr(jn:doc(...), "items"), [])   ← access
 *     └── Predicate
 *           └── ComparisonExpr(EQ, DerefExpr($x, "category"), "books")  ← predicate
 * </pre>
 *
 * After fusion (predicate metadata annotated on access node):
 * <pre>
 *   FilterExpr                                               ← preserved
 *     ├── ArrayAccess(DerefExpr(...))                        ← access (annotated)
 *     └── Predicate(...)                                     ← preserved for index matcher
 * </pre>
 *
 * <p>This walker annotates the FilterExpr and its access child with
 * predicate metadata so downstream stages (CostBasedStage, IndexMatching)
 * can route predicates to the cheapest available index.</p>
 *
 * <p>The annotations include:
 * <ul>
 *   <li>{@code fusedPredicate.operator} — comparison operator string (e.g., "ValueCompEQ")</li>
 *   <li>{@code fusedPredicate.fieldName} — the field being compared</li>
 *   <li>{@code fusedPredicate.count} — number of fused predicates</li>
 *   <li>{@code fusedPredicate.selectivity} — estimated combined selectivity</li>
 * </ul></p>
 */
public final class SelectAccessFusionWalker extends Walker {

  private boolean modified;

  public SelectAccessFusionWalker() {
    super();
  }

  /**
   * @return true if any fusion annotation was applied during the last walk
   */
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
    if (node.getType() != XQ.FilterExpr) {
      return node;
    }

    // FilterExpr must have exactly [accessExpr, Predicate]
    if (node.getChildCount() != 2) {
      return node;
    }

    final AST accessExpr = node.getChild(0);
    final AST predicateNode = node.getChild(1);

    if (predicateNode.getType() != XQ.Predicate || predicateNode.getChildCount() == 0) {
      return node;
    }

    // Access expression must be a path access (DerefExpr, ArrayAccess, or FunctionCall)
    if (!isAccessExpression(accessExpr)) {
      return node;
    }

    // Already fused — skip
    if (node.getProperty(CostProperties.FUSED_COUNT) != null) {
      return node;
    }

    final AST predicateBody = predicateNode.getChild(0);

    // Handle AND predicates: fuse multiple comparisons
    if (predicateBody.getType() == XQ.AndExpr) {
      return fuseAndPredicate(node, accessExpr, predicateBody);
    }

    // Handle single comparison
    if (predicateBody.getType() == XQ.ComparisonExpr || predicateBody.getChildCount() == 3) {
      return fuseSingleComparison(node, accessExpr, predicateBody);
    }

    return node;
  }

  /**
   * Fuse a single comparison predicate into the FilterExpr node.
   * Comparison structure: [operator, leftOperand, rightOperand]
   */
  private AST fuseSingleComparison(AST filterExpr, AST accessExpr, AST comparisonExpr) {
    if (comparisonExpr.getChildCount() != 3) {
      return filterExpr;
    }

    final String operator = comparisonExpr.getChild(0).getStringValue();
    if (operator == null) {
      return filterExpr;
    }

    // Extract the predicate field name from the DerefExpr in the comparison
    final String fieldName = extractPredicateFieldName(comparisonExpr);

    // Annotate the FilterExpr with fused predicate metadata
    filterExpr.setProperty(CostProperties.FUSED_COUNT, 1);
    filterExpr.setProperty(CostProperties.FUSED_OPERATOR, operator);
    if (fieldName != null) {
      filterExpr.setProperty(CostProperties.FUSED_FIELD_NAME, fieldName);
    }

    // Also annotate the access expression so CostBasedStage can find it
    accessExpr.setProperty(CostProperties.FUSED_HAS_PUSHDOWN, true);
    accessExpr.setProperty(CostProperties.FUSED_OPERATOR, operator);
    if (fieldName != null) {
      accessExpr.setProperty(CostProperties.FUSED_FIELD_NAME, fieldName);
    }

    modified = true;
    return filterExpr;
  }

  /**
   * Fuse an AND compound predicate — annotate with all child comparison metadata.
   */
  private AST fuseAndPredicate(AST filterExpr, AST accessExpr, AST andExpr) {
    final int predicateCount = andExpr.getChildCount();
    if (predicateCount == 0) {
      return filterExpr;
    }

    // Collect field names and operators from each child comparison
    final var operators = new String[predicateCount];
    final var fieldNames = new String[predicateCount];
    int validCount = 0;

    for (int i = 0; i < predicateCount; i++) {
      final AST child = andExpr.getChild(i);
      if (child.getChildCount() == 3) {
        final String op = child.getChild(0).getStringValue();
        if (op != null) {
          operators[validCount] = op;
          fieldNames[validCount] = extractPredicateFieldName(child);
          validCount++;
        }
      }
    }

    if (validCount == 0) {
      return filterExpr;
    }

    // Annotate the FilterExpr
    filterExpr.setProperty(CostProperties.FUSED_COUNT, validCount);
    filterExpr.setProperty(CostProperties.FUSED_OPERATOR, operators[0]);
    if (fieldNames[0] != null) {
      filterExpr.setProperty(CostProperties.FUSED_FIELD_NAME, fieldNames[0]);
    }
    if (validCount >= 2) {
      filterExpr.setProperty(CostProperties.FUSED_OPERATOR2, operators[1]);
      if (fieldNames[1] != null) {
        filterExpr.setProperty(CostProperties.FUSED_FIELD_NAME2, fieldNames[1]);
      }
    }

    // Annotate the access expression
    accessExpr.setProperty(CostProperties.FUSED_HAS_PUSHDOWN, true);
    accessExpr.setProperty(CostProperties.FUSED_PREDICATE_COUNT, validCount);

    modified = true;
    return filterExpr;
  }

  /**
   * Extract the field name from a comparison's left operand.
   * Typical structure: ComparisonExpr → child(1) → DerefExpr(..., "fieldName")
   */
  private String extractPredicateFieldName(AST comparisonExpr) {
    if (comparisonExpr.getChildCount() < 2) {
      return null;
    }
    final AST leftOperand = comparisonExpr.getChild(1);

    // Walk DerefExpr chain to find the leaf field name
    if (leftOperand.getType() == XQ.DerefExpr && leftOperand.getChildCount() >= 2) {
      return leftOperand.getChild(leftOperand.getChildCount() - 1).getStringValue();
    }

    return null;
  }

  private static boolean isAccessExpression(AST node) {
    final int type = node.getType();
    return type == XQ.DerefExpr || type == XQ.ArrayAccess || type == XQ.FunctionCall;
  }
}
