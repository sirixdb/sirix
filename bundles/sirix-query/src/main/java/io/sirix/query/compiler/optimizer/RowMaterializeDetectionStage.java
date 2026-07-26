package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sirix-side detection of COVERED-ROW pipelines (P5b stage 7c):
 *
 * <pre>
 *   for $r in $doc[] [where p]
 *   return {"a": $r.age, "d": $r.dept, ...}
 * </pre>
 *
 * i.e. a record constructor whose every value is a direct {@code $r.field} deref — rows a
 * covering projection can materialize from its own segments without touching the document
 * store. Annotates {@code SIRIX_ROW_MAT_*}; {@code SirixPipelineStrategy} consumes them
 * with the generic pipeline compiled alongside as the runtime fallback.
 *
 * <p><b>Filter safety</b> mirrors {@link GroupAggregateDetectionStage}: a selection
 * requires Brackit's representable-predicate annotation AND may reference only the loop
 * variable — otherwise the shape declines and the generic pipeline runs.
 */
public final class RowMaterializeDetectionStage implements Stage {

  public static final String ROW_MAT = "SIRIX_ROW_MAT";
  /** DISTINCT source columns needed (direct entries + computed operands). */
  public static final String ROW_MAT_FIELDS = "SIRIX_ROW_MAT_FIELDS";
  public static final String ROW_MAT_OUT_NAMES = "SIRIX_ROW_MAT_OUT_NAMES";
  /** Per record entry: index into ROW_MAT_FIELDS, or {@code -1} = computed entry. */
  public static final String ROW_MAT_DIRECT = "SIRIX_ROW_MAT_DIRECT";
  /** Per computed entry: postfix program ({@code null} slot for direct entries). */
  public static final String ROW_MAT_CODES = "SIRIX_ROW_MAT_CODES";
  /** Per computed entry: the program's constant pool ({@code null} for direct entries). */
  public static final String ROW_MAT_CONSTS = "SIRIX_ROW_MAT_CONSTS";

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
      forBind = forBind.getLastChild();
    }
    if (forBind == null || forBind.getType() != XQ.ForBind) {
      return;
    }
    // A plain `for $r in src` ForBind carries exactly [binding, source, next-op]. An
    // `allowing empty` marker or a positional `at $p` binding inserts extra children and
    // changes row semantics (empty-source row / extra variable) — decline explicitly
    // rather than rely on the child-count falling through downstream checks.
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
    if (returnExpr == null || returnExpr.getType() != XQ.ObjectConstructor
        || returnExpr.getChildCount() < 1) {
      return;
    }
    final int n = returnExpr.getChildCount();
    final List<String> sourceFields = new ArrayList<>(n);
    final String[] outNames = new String[n];
    final int[] direct = new int[n];
    final int[][] codes = new int[n][];
    final long[][] consts = new long[n][];
    final Set<String> seenNames = new HashSet<>();
    for (int i = 0; i < n; i++) {
      final AST kv = returnExpr.getChild(i);
      if (kv.getType() != XQ.KeyValueField || kv.getChildCount() != 2) {
        return;
      }
      final String name = stringLiteral(kv.getChild(0));
      if (name == null || !seenNames.add(name)) {
        return;
      }
      outNames[i] = name;
      final AST value = kv.getChild(1);
      final String field = loopVarDerefField(value, loopVar);
      if (field != null) {
        int idx = sourceFields.indexOf(field);
        if (idx < 0) {
          idx = sourceFields.size();
          sourceFields.add(field);
        }
        direct[i] = idx;
      } else if (value.getType() == XQ.ArithmeticExpr) {
        // Gap 2 (projections): a +/-/* tree over covered numeric fields — compiled to a
        // postfix program evaluated per row; any missing operand makes the record field
        // the empty sequence (the interpreter's arithmetic over empty is empty).
        final ComputedProgram.Program program =
            ComputedProgram.build(value, loopVar, sourceFields);
        if (program == null) {
          return;
        }
        direct[i] = -1;
        codes[i] = program.code();
        consts[i] = program.consts();
      } else {
        return;
      }
    }
    if (sourceFields.isEmpty()) {
      // All-constant record (every entry a literal-only program): no covered column to
      // scan — leave it to the interpreter rather than special-case a column-free path.
      return;
    }
    pipeExpr.setProperty(ROW_MAT, Boolean.TRUE);
    pipeExpr.setProperty(ROW_MAT_FIELDS, sourceFields.toArray(new String[0]));
    pipeExpr.setProperty(ROW_MAT_OUT_NAMES, outNames);
    pipeExpr.setProperty(ROW_MAT_DIRECT, direct);
    pipeExpr.setProperty(ROW_MAT_CODES, codes);
    pipeExpr.setProperty(ROW_MAT_CONSTS, consts);
  }

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

  private static String stringLiteral(final AST node) {
    if (node == null || node.getType() != XQ.Str) {
      return null;
    }
    final Object val = node.getValue();
    if (val instanceof Str str) {
      return str.stringValue();
    }
    return val instanceof String s ? s : null;
  }
}
