package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Sirix-side detection of COMPUTED-EXPRESSION AGGREGATE pipelines (gap item 2):
 *
 * <pre>
 *   sum|avg|min|max|count(
 *     for $r in $doc[] [where p]
 *     return $r.a * $r.b + 2   <i>(any +/-/* tree over loop-var derefs and integer literals)</i>
 *   )
 * </pre>
 *
 * The return expression compiles into a POSTFIX PROGRAM over the operand fields via
 * {@link ComputedProgram} (see its opcode contract). The serving kernel evaluates it with
 * {@code Math.*Exact} arithmetic and DECLINES on overflow — Brackit's interpreter
 * promotes overflowing {@code xs:integer} math to exact decimal, so the generic fallback
 * answers those exactly.
 *
 * <p>Rows where ANY operand field is missing contribute nothing: the interpreter's
 * arithmetic over the empty sequence is empty, so the row adds no item to the aggregated
 * sequence. {@code div/idiv/mod}, non-integer literals, and any other operand shape
 * decline. Only the pipe is annotated here; the aggregate FunctionCall wrapper is
 * recognized at translation time (SirixTranslator).
 */
public final class ComputedAggregateDetectionStage implements Stage {

  public static final String COMPUTED_AGG = "SIRIX_COMPUTED_AGG";
  public static final String COMPUTED_AGG_FIELDS = "SIRIX_COMPUTED_AGG_FIELDS";
  public static final String COMPUTED_AGG_CODE = "SIRIX_COMPUTED_AGG_CODE";
  public static final String COMPUTED_AGG_CONSTS = "SIRIX_COMPUTED_AGG_CONSTS";

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
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(node.getChild(i));
    }
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
    // Plain `for $r in src` only — `allowing empty` / positional `at $p` change semantics.
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
    AST current = forBind.getLastChild();
    for (; current != null && current.getType() != XQ.End; current = current.getLastChild()) {
      if (current.getType() == XQ.Selection) {
        if (!onlyReferencesVar(current.getChild(0), loopVar)) {
          return;
        }
        hasSelection = true;
      } else {
        return; // let/group-by/order-by/count/join — not this shape
      }
    }
    if (current == null || current.getChildCount() < 1) {
      return;
    }
    if (hasSelection && pipeExpr.getProperty(PREDICATE_TREE) == null) {
      return;
    }
    if (pipeExpr.getProperty(SOURCE_PATH) == null) {
      return;
    }
    final AST returnExpr = current.getChild(0);
    if (returnExpr == null || returnExpr.getType() != XQ.ArithmeticExpr) {
      return; // bare derefs are Brackit's own VECTORIZED_AGGREGATE shape — not ours
    }
    final List<String> fields = new ArrayList<>(4);
    final ComputedProgram.Program program = ComputedProgram.build(returnExpr, loopVar, fields);
    if (program == null || fields.isEmpty()) {
      // fields.isEmpty(): an all-constant return (sum(... return 2*3)) needs no columns —
      // the scan kernel rightly rejects zero operands, so decline at detection instead of
      // burning a per-execution failure. The interpreter handles it fine.
      return;
    }
    pipeExpr.setProperty(COMPUTED_AGG, Boolean.TRUE);
    pipeExpr.setProperty(COMPUTED_AGG_FIELDS, fields.toArray(new String[0]));
    pipeExpr.setProperty(COMPUTED_AGG_CODE, program.code());
    pipeExpr.setProperty(COMPUTED_AGG_CONSTS, program.consts());
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
