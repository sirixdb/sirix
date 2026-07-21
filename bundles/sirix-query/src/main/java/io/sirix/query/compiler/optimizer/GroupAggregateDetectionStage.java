package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sirix-side detection of PER-GROUP AGGREGATE pipelines (P5b stage 7a) — the shape
 * Brackit's own {@code VectorizedGroupByDetection} does not cover:
 *
 * <pre>
 *   for $r in $doc[] [where p]
 *   let $d := $r.dept
 *   group by $d
 *   return {"dept": $d, "n": count($r), "total": sum($r.age), ...}
 * </pre>
 *
 * i.e. one or more string group keys (gap item 1a widened the original single-key shape;
 * e.g. {@code group by $d, $c}) plus any mix of {@code count($r)} and
 * {@code sum|min|max|avg($r.field)} entries. Annotates {@code SIRIX_GROUP_AGG_*}
 * properties on the pipe expression, which {@code SirixPipelineStrategy} consumes to
 * emit a projection-served expression (with the generic pipeline compiled alongside as
 * the runtime fallback).
 *
 * <p><b>Filter safety.</b> This stage runs AFTER Brackit's detection walker, which
 * annotates {@code VECTORIZED_PREDICATE_TREE} whenever every {@code where} clause is
 * representable. A pipeline with a selection but WITHOUT that annotation must never be
 * served (the filter would be silently dropped) — it declines here.
 *
 * <p><b>Shape limits (deliberate).</b> At most {@code MAX_GROUP_KEYS} group keys; the
 * key entries must be the FIRST record fields, one {@code VariableRef} per group var,
 * each group var exactly once (record field order is part of the serialized answer);
 * aggregate arguments must be direct {@code $r.field} derefs; {@code order by} in the
 * pipe declines (group emission order is first-appearance, not sorted).
 */
public final class GroupAggregateDetectionStage implements Stage {

  public static final String GROUP_AGG = "SIRIX_GROUP_AGG";
  public static final String GROUP_AGG_GROUP_FIELDS = "SIRIX_GROUP_AGG_GROUP_FIELDS";
  public static final String GROUP_AGG_KEY_NAMES = "SIRIX_GROUP_AGG_KEY_NAMES";
  public static final String GROUP_AGG_FUNCS = "SIRIX_GROUP_AGG_FUNCS";
  public static final String GROUP_AGG_FIELDS = "SIRIX_GROUP_AGG_FIELDS";
  public static final String GROUP_AGG_OUT_NAMES = "SIRIX_GROUP_AGG_OUT_NAMES";

  /** Mirrors the kernel's packed-key bound (ProjectionIndexByteScan.MAX_GROUP_COLUMNS). */
  private static final int MAX_GROUP_KEYS = 5;

  /** Brackit's annotations, reused here (same property keys as its detection walker). */
  private static final String PREDICATE_TREE = "VECTORIZED_PREDICATE_TREE";
  private static final String SOURCE_PATH = "VECTORIZED_SOURCE_PATH_PREFIX";

  private static final Set<String> VALUE_FUNCS = Set.of("sum", "min", "max", "avg");

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
    // Plain `for $r in src` only: `allowing empty` emits an empty-source tuple (one
    // group record over ZERO rows) and a positional `at $p` adds a variable — both
    // change row semantics. Decline EXPLICITLY rather than rely on the source-path
    // walker happening not to annotate shifted-child shapes.
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
    final List<QNm> letVars = new ArrayList<>();
    final List<String> letFields = new ArrayList<>();
    final List<QNm> groupSpecVars = new ArrayList<>();
    boolean hasGroupBy = false;
    boolean hasSelection = false;
    AST current = forBind.getLastChild();
    for (; current != null && current.getType() != XQ.End; current = current.getLastChild()) {
      switch (current.getType()) {
        case XQ.Selection -> {
          // A selection AFTER group-by is HAVING-shaped (existential over the grouped
          // sequence) — serving it as a pre-group ROW filter changes counts and group
          // membership. Only pre-group selections are servable.
          if (hasGroupBy) {
            return;
          }
          // The predicate tree brackit annotates records FIELD names only, not deref
          // BASES — a where over some OTHER variable's field would be silently served
          // as a filter on the loop var. Require every variable in the selection to BE
          // the loop var.
          if (!onlyReferencesVar(current.getChild(0), loopVar)) {
            return;
          }
          hasSelection = true;
        }
        case XQ.LetBind -> {
          // A let AFTER group-by can shadow a group var (or feed the return) with a
          // per-group value the scan never sees — only pre-group lets are servable.
          if (hasGroupBy) {
            return;
          }
          if (current.getChildCount() < 2) {
            return;
          }
          final String field = loopVarDerefField(current.getChild(1), loopVar);
          if (field == null) {
            return; // a let we can't model — the served scan would not see it
          }
          final QNm letVar = bindingVarName(current);
          // Shadowing declines: a re-bound let var resolves to its LAST binding in the
          // interpreter while indexOf() would find the FIRST — and a let shadowing the
          // loop var changes every later deref. Both must never be served.
          if (letVar == null || letVar.equals(loopVar) || letVars.contains(letVar)) {
            return;
          }
          letVars.add(letVar);
          letFields.add(field);
        }
        case XQ.GroupBy -> {
          if (hasGroupBy) {
            // A SECOND group-by re-groups the grouped stream (and type-errors on >1-item
            // keys) — concatenating its specs into one joint grouping would replace that
            // error with an answer. Decline.
            return;
          }
          hasGroupBy = true;
          for (int i = 0; i < current.getChildCount(); i++) {
            final AST child = current.getChild(i);
            if (child.getType() != XQ.GroupBySpec) {
              continue;
            }
            final AST ref = child.getChildCount() > 0 ? child.getChild(0) : null;
            if (ref == null || ref.getType() != XQ.VariableRef
                || !(ref.getValue() instanceof QNm var)) {
              return;
            }
            groupSpecVars.add(var);
          }
        }
        default -> {
          return; // order-by / count / joins — not this shape
        }
      }
    }
    final int keyCount = groupSpecVars.size();
    if (!hasGroupBy || keyCount < 1 || keyCount > MAX_GROUP_KEYS || current == null
        || current.getChildCount() < 1) {
      return;
    }
    // Duplicate group-spec vars (group by $d, $d) — degenerate; leave to the interpreter.
    if (new HashSet<>(groupSpecVars).size() != keyCount) {
      return;
    }
    // Filter safety: a selection without Brackit's representable-predicate annotation
    // must never be served — the filter would silently vanish.
    if (hasSelection && pipeExpr.getProperty(PREDICATE_TREE) == null) {
      return;
    }
    if (pipeExpr.getProperty(SOURCE_PATH) == null) {
      return;
    }
    final AST returnExpr = current.getChild(0);
    if (returnExpr == null || returnExpr.getType() != XQ.ObjectConstructor
        || returnExpr.getChildCount() < keyCount + 1) {
      return;
    }
    // The first keyCount entries must be the group keys — one VariableRef per group var,
    // each group var exactly once, in any order. Record-entry order defines the served
    // key emission order (field order is answer shape).
    final String[] keyNames = new String[keyCount];
    final String[] groupFields = new String[keyCount];
    final Set<String> seenNames = new HashSet<>();
    final Set<QNm> seenGroupVars = new HashSet<>();
    for (int i = 0; i < keyCount; i++) {
      final AST keyEntry = returnExpr.getChild(i);
      final String keyName = kvName(keyEntry);
      if (keyName == null || !seenNames.add(keyName)) {
        return;
      }
      final AST keyValue = keyEntry.getChild(1);
      if (keyValue.getType() != XQ.VariableRef
          || !(keyValue.getValue() instanceof QNm keyVar)
          || !groupSpecVars.contains(keyVar) || !seenGroupVars.add(keyVar)) {
        return;
      }
      final int letIdx = letVars.indexOf(keyVar);
      if (letIdx < 0) {
        return;
      }
      keyNames[i] = keyName;
      groupFields[i] = letFields.get(letIdx);
    }
    final int aggCount = returnExpr.getChildCount() - keyCount;
    final String[] funcs = new String[aggCount];
    final String[] fields = new String[aggCount];
    final String[] outNames = new String[aggCount];
    for (int i = 0; i < aggCount; i++) {
      final AST entry = returnExpr.getChild(keyCount + i);
      final String name = kvName(entry);
      if (name == null || !seenNames.add(name)) {
        return;
      }
      final AST call = entry.getChild(1);
      if (call.getType() != XQ.FunctionCall || call.getChildCount() != 1
          || !(call.getValue() instanceof QNm fn)) {
        return;
      }
      // Built-in aggregate functions ONLY: a user-defined function whose LOCAL name is
      // sum/min/max/avg/count (e.g. local:sum) must never be served with fn:* semantics.
      // Unprefixed calls resolve to Brackit's JSONiq default-function namespace; fn:*
      // stays the XQuery namespace — both are the builtins.
      final String ns = fn.getNamespaceURI();
      if (ns != null && !ns.isEmpty()
          && !Namespaces.FN_NSURI.equals(ns) && !Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
        return;
      }
      final String func = fn.getLocalName();
      final AST arg = call.getChild(0);
      if ("count".equals(func)) {
        if (arg.getType() != XQ.VariableRef || !loopVar.equals(arg.getValue())) {
          return;
        }
        fields[i] = null;
      } else if (VALUE_FUNCS.contains(func)) {
        final String field = loopVarDerefField(arg, loopVar);
        if (field == null) {
          return;
        }
        fields[i] = field;
      } else {
        return;
      }
      funcs[i] = func;
      outNames[i] = name;
    }
    pipeExpr.setProperty(GROUP_AGG, Boolean.TRUE);
    pipeExpr.setProperty(GROUP_AGG_GROUP_FIELDS, groupFields);
    pipeExpr.setProperty(GROUP_AGG_KEY_NAMES, keyNames);
    pipeExpr.setProperty(GROUP_AGG_FUNCS, funcs);
    pipeExpr.setProperty(GROUP_AGG_FIELDS, fields);
    pipeExpr.setProperty(GROUP_AGG_OUT_NAMES, outNames);
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

  /** Key-value entry's string-literal name, else {@code null}. */
  private static String kvName(final AST kv) {
    if (kv.getType() != XQ.KeyValueField || kv.getChildCount() != 2) {
      return null;
    }
    final AST nameNode = kv.getChild(0);
    if (nameNode.getType() != XQ.Str) {
      return null;
    }
    final Object val = nameNode.getValue();
    if (val instanceof Str str) {
      return str.stringValue();
    }
    return val instanceof String s ? s : null;
  }
}
