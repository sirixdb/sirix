package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.XQExt;

/**
 * Recognizes independent single-value-equality semi/anti joins before FLWOR pipelining. The marker
 * is consumed by the Sirix pipeline translator as a physical membership join; all hash state
 * belongs to its cursor, never to a compiled expression or a tuple slot. Unsupported comparison
 * domains are evaluated by the original predicate. General comparisons, residual predicates,
 * typed/positional bindings and possibly empty returns keep their original plan.
 */
public final class HashMembershipStage implements Stage {
  public static final String ENABLED_PROPERTY = "sirix.optimizer.hashMembership";
  public static final String FIELD = "sirix.membership.field";
  public static final String ANTI = "sirix.membership.anti";

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    if (!"false".equalsIgnoreCase(System.getProperty(ENABLED_PROPERTY))) {
      walk(ast);
    }
    return ast;
  }

  private static void walk(final AST node) {
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(node.getChild(i));
    }
    if (node.getType() != XQ.FlowrExpr) {
      return;
    }
    for (int i = 0; i + 1 < node.getChildCount(); i++) {
      final AST outer = node.getChild(i);
      final QNm outerName = bindingName(outer, XQ.ForClause);
      final AST where = node.getChild(i + 1);
      if (outerName == null || where.getType() != XQ.WhereClause || where.getChildCount() != 1) {
        continue;
      }
      final Membership match = membership(where.getChild(0), outerName, false);
      if (match == null) {
        continue;
      }
      final AST probe = new AST(XQExt.HashMembershipJoin, "HashMembershipJoin");
      probe.addChild(match.source.copyTree());
      probe.addChild(match.scope.copyTree());
      probe.addChild(match.outerKey.copyTree());
      probe.addChild(where.getChild(0).copyTree());
      probe.setProperty(FIELD, match.field);
      probe.setProperty(ANTI, match.anti);
      where.replaceChild(0, probe);
    }
  }

  private static Membership membership(final AST expression, final QNm outerName, final boolean negate) {
    final AST node = unwrap(expression);
    if (builtin(node, "not")) {
      return membership(node.getChild(0), outerName, !negate);
    }
    if (builtin(node, "empty") || builtin(node, "exists")) {
      final AST inner = unwrap(node.getChild(0));
      if (inner.getType() != XQ.FlowrExpr || inner.getChildCount() != 3) {
        return null;
      }
      final AST loop = inner.getChild(0);
      final QNm innerName = bindingName(loop, XQ.ForClause);
      final AST where = inner.getChild(1);
      final AST returned = inner.getChild(2);
      if (innerName == null || where.getType() != XQ.WhereClause || where.getChildCount() != 1
          || returned.getType() != XQ.ReturnClause || returned.getChildCount() != 1) {
        return null;
      }
      final Membership match =
          equality(loop.getChild(1), where.getChild(0), innerName, outerName, builtin(node, "empty") != negate);
      if (match == null) {
        return null;
      }
      final AST value = unwrap(returned.getChild(0));
      // Returning the row itself or its equality key cannot erase a successful match.
      return isVariable(value, innerName) || isKey(value, innerName) && sameField(field(value), match.field)
          ? match
          : null;
    }
    if (node.getType() == XQ.QuantifiedExpr && node.getChildCount() == 3
        && node.getChild(0).getType() == XQ.SomeQuantifier) {
      final AST binding = node.getChild(1);
      final QNm innerName = bindingName(binding, XQ.QuantifiedBinding);
      if (innerName != null) {
        return equality(binding.getChild(1), node.getChild(2), innerName, outerName, negate);
      }
    }
    return null;
  }

  private static Membership equality(final AST source, final AST condition, final QNm innerName, final QNm outerName,
      final boolean anti) {
    final AST comparison = unwrap(condition);
    final AST scope = scope(source, outerName);
    if (scope == null || comparison.getType() != XQ.ComparisonExpr || comparison.getChildCount() != 3
        || comparison.getChild(0).getType() != XQ.ValueCompEQ) {
      return null;
    }
    final AST left = unwrap(comparison.getChild(1));
    final AST right = unwrap(comparison.getChild(2));
    if (isKey(left, innerName) && isKey(right, outerName)) {
      return new Membership(source, scope, field(left), right, anti);
    }
    if (isKey(right, innerName) && isKey(left, outerName)) {
      return new Membership(source, scope, field(right), left, anti);
    }
    return null;
  }

  private static QNm bindingName(final AST node, final int type) {
    if (node.getType() != type || node.getChildCount() != 2) {
      return null;
    }
    final AST binding = node.getChild(0);
    return binding.getType() == XQ.TypedVariableBinding && binding.getChildCount() == 1
        && binding.getChild(0).getValue() instanceof QNm name
            ? name
            : null;
  }

  /**
   * The independent variable the source reads. It is the lookup's scope: its value is identical for
   * every row of the outer {@code for}, and changes exactly when an enclosing binding changes.
   */
  private static AST scope(final AST source, final QNm outerName) {
    AST base = unwrap(source);
    if (base.getType() == XQ.ArrayAccess && base.getChildCount() == 2 && base.getChild(1).getType() == XQ.SequenceExpr
        && base.getChild(1).getChildCount() == 0) {
      base = unwrap(base.getChild(0));
    }
    return base.getType() == XQ.VariableRef && !outerName.equals(base.getValue())
        ? base
        : null;
  }

  private static boolean isKey(final AST key, final QNm name) {
    return isVariable(key, name) || key.getType() == XQ.DerefExpr && key.getChildCount() == 2
        && isVariable(unwrap(key.getChild(0)), name) && key.getChild(1).getType() == XQ.QNm;
  }

  private static QNm field(final AST key) {
    return key.getType() == XQ.DerefExpr
        ? (QNm) key.getChild(1).getValue()
        : null;
  }

  private static boolean sameField(final QNm first, final QNm second) {
    return first == null
        ? second == null
        : first.equals(second);
  }

  private static boolean isVariable(final AST node, final QNm name) {
    return node.getType() == XQ.VariableRef && name.equals(node.getValue());
  }

  private static AST unwrap(final AST node) {
    AST current = node;
    while (current.getType() == XQ.ParenthesizedExpr && current.getChildCount() == 1) {
      current = current.getChild(0);
    }
    return current;
  }

  private static boolean builtin(final AST node, final String localName) {
    if (node.getType() != XQ.FunctionCall || node.getChildCount() != 1 || !(node.getValue() instanceof QNm name)
        || !localName.equals(name.getLocalName())) {
      return false;
    }
    final String namespace = name.getNamespaceURI();
    return namespace == null || namespace.isEmpty() || Namespaces.FN_NSURI.equals(namespace)
        || Namespaces.DEFAULT_FN_NSURI.equals(namespace);
  }

  private record Membership(AST source, AST scope, QNm field, AST outerKey, boolean anti) {
  }
}
