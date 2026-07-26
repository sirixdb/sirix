package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Sirix-side detection of SORTED-SCAN pipelines (P5b stage 7b; gap item 1b widened the
 * original single order key to N):
 *
 * <pre>
 *   for $r in $doc[] [where p]
 *   order by $r.f1 [ascending|descending], $r.f2 [ascending|descending], ...
 *   return $r
 * </pre>
 *
 * Annotates {@code SIRIX_SORTED_*}; {@code SirixPipelineStrategy} consumes them with the
 * generic pipeline compiled alongside as the runtime fallback. This stage subsumes the
 * original consumption of Brackit's {@code VECTORIZED_ORDERBY} annotation (which only
 * covers a single spec, and whose direction property is unusable) — direction is read
 * straight from each {@code OrderBySpec}'s {@code OrderByKind} modifier.
 *
 * <p><b>Declines (all deliberate, mirroring the single-key rules).</b> A second
 * {@code order by} clause; an explicit {@code empty greatest/least} modifier; a collation
 * or any other unrecognized spec modifier; {@code let} bindings in the chain (spec keys
 * must be direct {@code $r.field} derefs); a selection without Brackit's
 * representable-predicate annotation; {@code allowing empty}/positional {@code at}
 * ForBind modifiers.
 */
public final class SortedScanDetectionStage implements Stage {

  public static final String SORTED_SCAN = "SIRIX_SORTED_SCAN";
  public static final String SORTED_FIELDS = "SIRIX_SORTED_FIELDS";
  public static final String SORTED_DESC = "SIRIX_SORTED_DESC";

  /**
   * TOP-K pushdown (gap item 3): when a sorted pipe's SOLE consumer is
   * {@code fn:subsequence(pipe, start, length)} with positive integer literals, the pipe
   * only ever needs its first {@code start+length-1} items — {@code fn:subsequence} never
   * pulls past that position. The limit is annotated here and served via bounded heap
   * selection instead of a full sort.
   */
  public static final String SORTED_LIMIT = "SIRIX_SORTED_LIMIT";

  private static final String PREDICATE_TREE = "VECTORIZED_PREDICATE_TREE";
  private static final String SOURCE_PATH = "VECTORIZED_SOURCE_PATH_PREFIX";

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    walk(ast);
    return ast;
  }

  private void walk(final AST node) {
    if (node == null) {
      return;
    }
    if (node.getType() == XQ.PipeExpr) {
      tryAnnotate(node);
    } else if (node.getType() == XQ.FunctionCall) {
      tryAnnotateSubsequenceLimit(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(node.getChild(i));
    }
  }

  /** {@code fn:subsequence(<pipe>, <int-literal start>, <int-literal length>)} → limit. */
  private static void tryAnnotateSubsequenceLimit(final AST call) {
    if (call.getChildCount() != 3 || !(call.getValue() instanceof QNm fn)
        || !"subsequence".equals(fn.getLocalName())) {
      return;
    }
    // Built-in fn:subsequence only: unprefixed calls resolve to the JSONiq default
    // function namespace, fn:* to the XQuery one.
    final String ns = fn.getNamespaceURI();
    if (ns != null && !ns.isEmpty() && !Namespaces.FN_NSURI.equals(ns)
        && !Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
      return;
    }
    final AST pipe = call.getChild(0);
    if (pipe.getType() != XQ.PipeExpr) {
      return;
    }
    final long start = intLiteral(call.getChild(1));
    final long length = intLiteral(call.getChild(2));
    if (start < 1 || length < 0) {
      return; // non-literal or non-positive: subsequence semantics get subtle — decline
    }
    final long limit = start + length - 1; // literals are 32-bit — no overflow
    if (limit > Integer.MAX_VALUE) {
      return;
    }
    pipe.setProperty(SORTED_LIMIT, limit);
  }

  /** Integer literal value of an {@link XQ#Int} node, or {@code -1} if not one. */
  private static long intLiteral(final AST node) {
    if (node == null || node.getType() != XQ.Int) {
      return -1;
    }
    final Object v = node.getValue();
    if (v instanceof Int32 i32) {
      return i32.longValue();
    }
    return v instanceof Int64 i64 ? i64.longValue() : -1;
  }

  private void tryAnnotate(final AST pipeExpr) {
    if (pipeExpr.getChildCount() < 1) {
      return;
    }
    final AST chain = pipeExpr.getChild(0);
    if (chain.getType() != XQ.Start || chain.getChildCount() < 1) {
      return;
    }
    AST forBind = chain.getLastChild();
    while (forBind != null && forBind.getType() == XQ.LetBind) {
      forBind = forBind.getLastChild(); // skip leading lets (e.g. `let $doc := jn:doc(...)`)
    }
    if (forBind == null || forBind.getType() != XQ.ForBind) {
      return;
    }
    // Plain `for $r in src` only: [binding, source, next-op]. `allowing empty` or a
    // positional `at $p` binding changes row semantics — decline explicitly.
    if (forBind.getChildCount() != 3
        || forBind.getChild(0).getType() != XQ.TypedVariableBinding
        || forBind.getChild(1).getType() == XQ.AllowingEmpty
        || forBind.getChild(1).getType() == XQ.TypedVariableBinding) {
      return;
    }
    final QNm loopVar = bindingVarName(forBind);
    if (loopVar == null) {
      return;
    }
    boolean hasSelection = false;
    AST orderBy = null;
    AST current = forBind.getLastChild();
    for (; current != null && current.getType() != XQ.End; current = current.getLastChild()) {
      switch (current.getType()) {
        case XQ.Selection -> {
          if (orderBy != null) {
            // A selection AFTER order-by filters the sorted stream — not this shape.
            return;
          }
          if (!onlyReferencesVar(current.getChild(0), loopVar)) {
            return;
          }
          hasSelection = true;
        }
        case XQ.LetBind -> {
          // A let is servable ONLY when its bound expression provably cannot raise: the
          // generic pipeline evaluates every let per row, so `let $x := $r.a idiv 0`
          // makes the interpreter ERROR while serving (which skips lets — specs and
          // return use only the loop var) would answer. Direct `$r.field` derefs never
          // raise (missing → empty); anything else declines. A let shadowing the loop
          // var declines too (later derefs would read the let-bound value, not the row).
          // Post-order-by lets stay declined (strict).
          if (orderBy != null) {
            return;
          }
          final QNm letVar = bindingVarName(current);
          if (letVar == null || letVar.equals(loopVar)) {
            return;
          }
          if (current.getChildCount() < 2
              || loopVarDerefField(current.getChild(1), loopVar) == null) {
            return;
          }
        }
        case XQ.OrderBy -> {
          if (orderBy != null) {
            return; // a SECOND order-by re-sorts — decline rather than sort wrongly
          }
          orderBy = current;
        }
        default -> {
          return; // group-by/count/join — not this shape
        }
      }
    }
    if (orderBy == null || current == null || current.getChildCount() < 1) {
      return;
    }
    if (hasSelection && pipeExpr.getProperty(PREDICATE_TREE) == null) {
      return;
    }
    if (pipeExpr.getProperty(SOURCE_PATH) == null) {
      return;
    }
    final AST returnExpr = current.getChild(0);
    if (returnExpr == null || returnExpr.getType() != XQ.VariableRef
        || !loopVar.equals(returnExpr.getValue())) {
      return;
    }
    final List<String> fields = new ArrayList<>(4);
    final List<Boolean> descList = new ArrayList<>(4);
    for (int i = 0; i < orderBy.getChildCount(); i++) {
      final AST spec = orderBy.getChild(i);
      if (spec.getType() != XQ.OrderBySpec) {
        continue; // trailing next-op child
      }
      if (spec.getChildCount() < 1) {
        return;
      }
      final String field = loopVarDerefField(spec.getChild(0), loopVar);
      if (field == null) {
        return;
      }
      boolean descending = false;
      for (int m = 1; m < spec.getChildCount(); m++) {
        final AST modifier = spec.getChild(m);
        if (modifier.getType() == XQ.OrderByKind && modifier.getChildCount() > 0) {
          final int kind = modifier.getChild(0).getType();
          if (kind == XQ.DESCENDING) {
            descending = true;
          } else if (kind != XQ.ASCENDING) {
            return;
          }
        } else {
          // OrderByEmptyMode (empty greatest/least), Collation, anything unrecognized:
          // fail closed — serving must not silently ignore a modifier it cannot honor.
          return;
        }
      }
      fields.add(field);
      descList.add(descending);
    }
    if (fields.isEmpty()) {
      return;
    }
    final String[] fieldsArr = fields.toArray(new String[0]);
    final boolean[] descArr = new boolean[descList.size()];
    for (int i = 0; i < descArr.length; i++) {
      descArr[i] = descList.get(i);
    }
    pipeExpr.setProperty(SORTED_SCAN, Boolean.TRUE);
    pipeExpr.setProperty(SORTED_FIELDS, fieldsArr);
    pipeExpr.setProperty(SORTED_DESC, descArr);
  }

  /** Every {@link XQ#VariableRef} in the subtree is {@code var} — no foreign variables. */
  private static boolean onlyReferencesVar(final AST node, final QNm var) {
    if (node == null) {
      return false;
    }
    if (node.getType() == XQ.VariableRef) {
      return var.equals(node.getValue());
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (!onlyReferencesVar(node.getChild(i), var)) {
        return false;
      }
    }
    return true;
  }

  /** {@code $loopVar.field} direct deref → field local name, else {@code null}. */
  private static String loopVarDerefField(final AST expr, final QNm loopVar) {
    if (expr == null || expr.getType() != XQ.DerefExpr || expr.getChildCount() < 2) {
      return null;
    }
    final AST base = expr.getChild(0);
    if (base.getType() != XQ.VariableRef || !loopVar.equals(base.getValue())) {
      return null;
    }
    final Object name = expr.getChild(expr.getChildCount() - 1).getValue();
    if (name instanceof QNm qnm) {
      return qnm.getLocalName();
    }
    return name instanceof String s ? s : null;
  }

  /** First child of a binding node is the typed variable binding; its first child names the var. */
  private static QNm bindingVarName(final AST bindNode) {
    if (bindNode.getChildCount() < 1) {
      return null;
    }
    final AST binding = bindNode.getChild(0);
    if (binding.getChildCount() < 1) {
      return null;
    }
    return binding.getChild(0).getValue() instanceof QNm qnm ? qnm : null;
  }
}
