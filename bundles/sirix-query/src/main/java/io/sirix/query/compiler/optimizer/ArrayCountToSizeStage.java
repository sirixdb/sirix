package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;
import io.sirix.query.function.jn.SirixArraySize;

/**
 * Rewrites {@code count(for $x in E[] return $x)} and {@code count(E[])} into
 * {@code jn:sirix-array-size(E)}.
 *
 * <p>
 * Counting an array by unboxing it walks every member: on the 3.48 M-record comparison corpus that
 * is a 325–360 ms full scan, against <b>0.2 ms</b> for the stored-size accessor, because a JSON
 * array node stores its child count and the answer is a single node read. PostgreSQL cannot do this
 * — an MVCC heap has no O(1) row count, which is why {@code SELECT count(*)} scans there too — so
 * this is a structural advantage that was simply not being reached.
 *
 * <p>
 * <b>Why the rewrite is sound.</b> The {@code return} clause must be exactly the loop variable and
 * the pipeline must carry no other clause: a {@code where}, {@code let}, {@code group by},
 * {@code order by}, {@code count}, positional {@code at $p}, or {@code allowing empty} changes
 * either the count or the binding, and every one is rejected below. At runtime
 * {@link SirixArraySize} takes the O(1) child-count arm only for a direct Sirix-backed array; every
 * other operand is delegated to Brackit's own array-access implementation before it is counted.
 * This preserves Brackit's runtime sequence dispatch and error behavior for the general case.
 */
public final class ArrayCountToSizeStage implements Stage {

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    final AST replaced = tryRewrite(ast);
    if (replaced != null) {
      return replaced;
    }
    walk(ast);
    return ast;
  }

  private void walk(final AST node) {
    for (int i = 0; i < node.getChildCount(); i++) {
      final AST child = node.getChild(i);
      final AST replacement = tryRewrite(child);
      if (replacement != null) {
        node.replaceChild(i, replacement);
      } else {
        walk(child);
      }
    }
  }

  /** @return the stored-size call when {@code node} matches the pattern, else {@code null}. */
  private static AST tryRewrite(final AST node) {
    if (node.getType() != XQ.FunctionCall || node.getChildCount() != 1 || !(node.getValue() instanceof QNm fn)
        || !isBuiltinCount(fn)) {
      return null;
    }
    final AST arrayExpr = countableArrayExpr(node.getChild(0));
    if (arrayExpr == null) {
      return null;
    }
    final AST size = new AST(XQ.FunctionCall, SirixArraySize.SIRIX_ARRAY_SIZE);
    // copyTree(), not copy(): copy() is shallow, so an array whose expression has children — a
    // literal `[1,2,3]`, a nested deref — arrives at the size function stripped of its elements and
    // reports size 0. A variable reference has no children, which is exactly why the corpus query
    // looked correct while `count(for $m in [1,2,3][] return $m)` silently returned 0.
    size.addChild(arrayExpr.copyTree());
    return size;
  }

  /**
   * A user-defined {@code local:count} must never be given {@code fn:count} semantics. Unprefixed
   * calls resolve to Brackit's JSONiq default-function namespace; {@code fn:} stays the XQuery one.
   */
  private static boolean isBuiltinCount(final QNm fn) {
    if (!"count".equals(fn.getLocalName())) {
      return false;
    }
    final String ns = fn.getNamespaceURI();
    return ns == null || ns.isEmpty() || Namespaces.FN_NSURI.equals(ns) || Namespaces.DEFAULT_FN_NSURI.equals(ns);
  }

  /**
   * Unwraps the argument of {@code count(...)} to the array expression {@code E}, for the two shapes
   * whose item count is the array's size: the bare unbox {@code E[]}, and the identity pipeline
   * {@code for $x in E[] return $x}. Anything else returns {@code null}.
   */
  private static AST countableArrayExpr(final AST arg) {
    if (isUnboxAll(arg)) {
      return arg.getChild(0);
    }
    if (arg.getType() != XQ.PipeExpr || arg.getChildCount() < 1) {
      return null;
    }
    final AST start = arg.getChild(0);
    if (start.getType() != XQ.Start || start.getChildCount() != 1) {
      return null;
    }
    final AST forBind = start.getChild(0);
    // Exactly three children: the binding, the source, and End. A positional `at $p` or
    // `allowing empty` adds one, and both change what the pipeline yields.
    if (forBind.getType() != XQ.ForBind || forBind.getChildCount() != 3) {
      return null;
    }
    final AST binding = forBind.getChild(0);
    if (binding.getType() != XQ.TypedVariableBinding || binding.getChildCount() != 1) {
      return null;
    }
    final AST source = forBind.getChild(1);
    if (!isUnboxAll(source)) {
      return null;
    }
    // The pipeline must end immediately: any Selection/LetBind/GroupBy/OrderBy/Count in between
    // would alter the cardinality or the binding.
    final AST end = forBind.getChild(2);
    if (end.getType() != XQ.End || end.getChildCount() != 1) {
      return null;
    }
    final AST returned = end.getChild(0);
    if (returned.getType() != XQ.VariableRef || !sameVariable(binding.getChild(0), returned)) {
      return null;
    }
    return source.getChild(0);
  }

  /** {@code E[]} — an ArrayAccess whose index is the empty sequence, i.e. "all members". */
  private static boolean isUnboxAll(final AST node) {
    return node.getType() == XQ.ArrayAccess && node.getChildCount() == 2
        && node.getChild(1).getType() == XQ.SequenceExpr && node.getChild(1).getChildCount() == 0;
  }

  private static boolean sameVariable(final AST declared, final AST referenced) {
    final Object a = declared.getValue();
    final Object b = referenced.getValue();
    return a != null && a.equals(b);
  }
}
