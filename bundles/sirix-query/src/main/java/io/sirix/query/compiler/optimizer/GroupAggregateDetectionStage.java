package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
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
 * Sirix-side detection of PER-GROUP AGGREGATE pipelines (P5b stage 7a) — the shape Brackit's own
 * {@code VectorizedGroupByDetection} does not cover:
 *
 * <pre>
 *   for $r in $doc[] [where p]
 *   let $d := $r.dept
 *   group by $d
 *   return {"dept": $d, "n": count($r), "total": sum($r.age), ...}
 * </pre>
 *
 * i.e. one or more string group keys (gap item 1a widened the original single-key shape; e.g.
 * {@code group by $d, $c}) plus any mix of {@code count($r)} and {@code sum|min|max|avg($r.field)}
 * entries. Annotates {@code SIRIX_GROUP_AGG_*} properties on the pipe expression, which
 * {@code SirixPipelineStrategy} consumes to emit a projection-served expression (with the generic
 * pipeline compiled alongside as the runtime fallback).
 *
 * <p>
 * <b>Filter safety.</b> This stage runs AFTER Brackit's detection walker, which annotates
 * {@code VECTORIZED_PREDICATE_TREE} whenever every {@code where} clause is representable. A
 * pipeline with a selection but WITHOUT that annotation must never be served (the filter would be
 * silently dropped) — it declines here.
 *
 * <p>
 * Also covers the form every analytical benchmark actually writes, where the aggregate is bound by
 * a POST-group {@code let} and the groups are sorted by it:
 *
 * <pre>
 *   for $r in $doc[] where $r.x != 0
 *   let $k := $r.dept
 *   group by $k
 *   let $c := count($r)
 *   order by $c descending
 *   return {"dept": $k, "count": $c}
 * </pre>
 *
 * After the group-by Brackit binds the loop var to the grouped SEQUENCE, so {@code count($r)} in a
 * post-group let means exactly what it means inside the return record.
 *
 * <p>
 * <b>Shape limits (deliberate).</b> At most {@code MAX_GROUP_KEYS} group keys; the key entries must
 * be the FIRST record fields, one {@code VariableRef} per group var, each group var exactly once
 * (record field order is part of the serialized answer); aggregate arguments must be a direct
 * {@code $r.field} deref or a pre-group let bound to one; every {@code order by} spec must be a
 * bare variable that the return record EMITS (anything else would have to be recomputed outside the
 * scan) and must follow the group-by — a pre-group {@code order by} reorders rows and so changes
 * which tuple is first in each group, which is the emission order the served path reproduces.
 */
public final class GroupAggregateDetectionStage implements Stage {

  public static final String GROUP_AGG = "SIRIX_GROUP_AGG";
  public static final String GROUP_AGG_GROUP_FIELDS = "SIRIX_GROUP_AGG_GROUP_FIELDS";
  public static final String GROUP_AGG_KEY_NAMES = "SIRIX_GROUP_AGG_KEY_NAMES";
  public static final String GROUP_AGG_FUNCS = "SIRIX_GROUP_AGG_FUNCS";
  public static final String GROUP_AGG_FIELDS = "SIRIX_GROUP_AGG_FIELDS";
  public static final String GROUP_AGG_OUT_NAMES = "SIRIX_GROUP_AGG_OUT_NAMES";
  /** {@code int[]}: for each order-by spec, the emitted record entry it sorts on. */
  public static final String GROUP_AGG_ORDER_INDEXES = "SIRIX_GROUP_AGG_ORDER_INDEXES";
  /** {@code boolean[]}: ascending per order-by spec. */
  public static final String GROUP_AGG_ORDER_ASC = "SIRIX_GROUP_AGG_ORDER_ASC";
  /** {@code boolean[]}: empty-least per order-by spec (Brackit's default is {@code true}). */
  public static final String GROUP_AGG_ORDER_EMPTY_LEAST = "SIRIX_GROUP_AGG_ORDER_EMPTY_LEAST";
  /**
   * {@code Boolean.TRUE} when EVERY group key is a pre-group let bound to a LITERAL — the grouping
   * partitions nothing, the whole matching input is one group, and the aggregates fold in a single
   * scalar pass (Q29's {@code let $g := 1 ... group by $g} shape). Key annotations are absent.
   */
  public static final String GROUP_AGG_CONST = "SIRIX_GROUP_AGG_CONST";
  /**
   * {@code long[]}: per emitted aggregate entry, the constant offset of a shifted operand —
   * {@code let $w := $r.f + k ... sum($w)} carries {@code k}, a plain deref carries {@code 0}.
   * {@code sum(f+k) = sum(f) + k·presentCount(f)}, {@code min/max/avg(f+k) = min/max/avg(f) + k}
   * (the let's arithmetic over a MISSING f is the empty sequence, so exactly the present rows
   * contribute — the same population the plain aggregate folds).
   */
  public static final String GROUP_AGG_OFFSETS = "SIRIX_GROUP_AGG_OFFSETS";

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
    if (forBind.getChildCount() != 3 || forBind.getChild(0).getType() != XQ.TypedVariableBinding
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
    // Per pre-group let: the constant added to the deref (0 for a plain `$r.field`). A key may
    // only bind to an offset-0 let — grouping by `f + 1` under field `f` would serve wrong keys.
    final List<Long> letOffsets = new ArrayList<>();
    // Pre-group lets bound to a LITERAL: constant group keys (`let $g := 1 ... group by $g`).
    final List<QNm> constLetVars = new ArrayList<>();
    final List<QNm> groupSpecVars = new ArrayList<>();
    // POST-group aggregate lets: `group by $k let $c := count($r)`. After the group-by, brackit
    // binds the loop var to the GROUPED sequence (the GroupBy node's AggregateSpec/SequenceAgg),
    // so `count($r)` / `sum($r.f)` here mean exactly what the same call means inside the return
    // record. Parallel lists, index-aligned.
    final List<QNm> postGroupVars = new ArrayList<>();
    final List<String> postGroupFuncs = new ArrayList<>();
    final List<String> postGroupFields = new ArrayList<>();
    final List<Long> postGroupOffsets = new ArrayList<>();
    // Group-by specs that named a CONSTANT let: they partition nothing.
    final List<QNm> constKeySpecs = new ArrayList<>();
    // ORDER BY specs, resolved to the variable each sorts on. Only reachable after the group-by.
    final List<QNm> orderVars = new ArrayList<>();
    final List<Boolean> orderAsc = new ArrayList<>();
    final List<Boolean> orderEmptyLeast = new ArrayList<>();
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
          if (current.getChildCount() < 2) {
            return;
          }
          final QNm letVar = bindingVarName(current);
          // Shadowing declines, before and after the group-by alike: a re-bound var resolves
          // to its LAST binding in the interpreter while indexOf() would find the FIRST, and
          // a let shadowing the loop var changes every later deref.
          if (letVar == null || letVar.equals(loopVar) || letVars.contains(letVar) || postGroupVars.contains(letVar)
              || constLetVars.contains(letVar)) {
            return;
          }
          if (hasGroupBy) {
            // A POST-group let is servable only when it binds an AGGREGATE over the grouped
            // loop var — a value the scan already computes. After the group-by brackit binds
            // the loop var to the grouped SEQUENCE, so `count($r)` / `sum($r.f)` here mean
            // exactly what the same call means inside the return record. Anything else (a
            // per-group expression, a constant, a reference the scan never sees) declines.
            if (groupSpecVars.contains(letVar)) {
              return;
            }
            final Agg agg = aggregateCall(current.getChild(1), loopVar, letVars, letFields, letOffsets);
            if (agg == null) {
              return;
            }
            postGroupVars.add(letVar);
            postGroupFuncs.add(agg.func());
            postGroupFields.add(agg.field());
            postGroupOffsets.add(agg.offset());
          } else {
            final AST bound = current.getChild(1);
            final String field = loopVarDerefField(bound, loopVar);
            if (field != null) {
              letVars.add(letVar);
              letFields.add(field);
              letOffsets.add(0L);
            } else if (bound.getType() == XQ.Int) {
              // A literal binding: a CONSTANT group key candidate (`let $g := 1`). Only a
              // group-by spec may consume it — an aggregate or record entry over it declines.
              constLetVars.add(letVar);
            } else {
              final ShiftedDeref shifted = shiftedDeref(bound, loopVar);
              if (shifted == null) {
                return; // a let we can't model — the served scan would not see it
              }
              letVars.add(letVar);
              letFields.add(shifted.field());
              letOffsets.add(shifted.offset());
            }
          }
        }
        case XQ.OrderBy -> {
          // Only AFTER the group-by. A pre-group order by reorders the ROWS, which changes
          // which tuple is first in each group and therefore the group emission order the
          // served path reproduces — it is not a post-pass over the groups.
          if (!hasGroupBy || !orderVars.isEmpty()) {
            return;
          }
          for (int i = 0; i < current.getChildCount(); i++) {
            final AST spec = current.getChild(i);
            if (spec.getType() != XQ.OrderBySpec) {
              // The pipeline SUCCESSOR is a child of this node too (the chain is nested, not a
              // sibling list) — skip it exactly as the group-by arm above skips it.
              continue;
            }
            if (spec.getChildCount() < 1) {
              return;
            }
            final AST key = spec.getChild(0);
            if (key.getType() != XQ.VariableRef || !(key.getValue() instanceof QNm orderVar)) {
              return; // only a bare variable — an expression would have to be re-evaluated
            }
            // Brackit's own defaults, mirrored from Compiler#orderModifier.
            boolean asc = true;
            boolean emptyLeast = true;
            for (int m = 1; m < spec.getChildCount(); m++) {
              final AST modifier = spec.getChild(m);
              if (modifier.getType() == XQ.OrderByKind) {
                asc = modifier.getChild(0).getType() == XQ.ASCENDING;
              } else if (modifier.getType() == XQ.OrderByEmptyMode) {
                emptyLeast = modifier.getChild(0).getType() == XQ.LEAST;
              } else if (modifier.getType() == XQ.Collation) {
                return; // only the codepoint collation exists, and it is the default anyway
              } else {
                return; // an unrecognised modifier must never be silently dropped
              }
            }
            orderVars.add(orderVar);
            orderAsc.add(asc);
            orderEmptyLeast.add(emptyLeast);
          }
          if (orderVars.isEmpty()) {
            return;
          }
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
            final AST ref = child.getChildCount() > 0
                ? child.getChild(0)
                : null;
            if (ref == null || ref.getType() != XQ.VariableRef || !(ref.getValue() instanceof QNm var)) {
              return;
            }
            if (constLetVars.contains(var)) {
              constKeySpecs.add(var);
            } else {
              groupSpecVars.add(var);
            }
          }
        }
        default -> {
          return; // order-by / count / joins — not this shape
        }
      }
    }
    final int keyCount = groupSpecVars.size();
    // CONSTANT grouping: every key is a literal-bound let, the matching input is ONE group.
    // Mixed constant + real keys stay declined — dropping the constant from a real grouping is
    // sound but is not this shape.
    final boolean constMode = keyCount == 0 && !constKeySpecs.isEmpty();
    if (!hasGroupBy || current == null || current.getChildCount() < 1) {
      return;
    }
    if (!constMode && (keyCount < 1 || keyCount > MAX_GROUP_KEYS || !constKeySpecs.isEmpty())) {
      return;
    }
    if (constMode && !orderVars.isEmpty()) {
      // One group: an order-by is vacuous, but honoring it here would mean re-deriving Brackit's
      // single-tuple ordering semantics for no gain — fail closed.
      return;
    }
    // Duplicate group-spec vars (group by $d, $d) — degenerate; leave to the interpreter.
    if (new HashSet<>(groupSpecVars).size() != keyCount || new HashSet<>(constKeySpecs).size() != constKeySpecs.size()) {
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
      if (keyValue.getType() != XQ.VariableRef || !(keyValue.getValue() instanceof QNm keyVar)
          || !groupSpecVars.contains(keyVar) || !seenGroupVars.add(keyVar)) {
        return;
      }
      final int letIdx = letVars.indexOf(keyVar);
      if (letIdx < 0 || letOffsets.get(letIdx) != 0L) {
        // A key bound to a SHIFTED let (`group by $w` with `$w := $r.f + 1`) would serve `f`'s
        // values as keys — off by the shift. Decline.
        return;
      }
      keyNames[i] = keyName;
      groupFields[i] = letFields.get(letIdx);
    }
    final int aggCount = returnExpr.getChildCount() - keyCount;
    final String[] funcs = new String[aggCount];
    final String[] fields = new String[aggCount];
    final String[] outNames = new String[aggCount];
    final long[] offsets = new long[aggCount];
    // Which emitted entry carries each post-group let var, so an order-by on that var resolves
    // to a field of the answer record instead of a value only the interpreter can recompute.
    final List<QNm> emittedPostGroupVars = new ArrayList<>();
    final List<Integer> emittedPostGroupAt = new ArrayList<>();
    for (int i = 0; i < aggCount; i++) {
      final AST entry = returnExpr.getChild(keyCount + i);
      final String name = kvName(entry);
      if (name == null || !seenNames.add(name)) {
        return;
      }
      final AST value = entry.getChild(1);
      final Agg agg;
      if (value.getType() == XQ.VariableRef && value.getValue() instanceof QNm valueVar) {
        // A reference to a post-group aggregate let: `let $c := count($r) ... return {"n": $c}`.
        final int postIdx = postGroupVars.indexOf(valueVar);
        if (postIdx < 0) {
          return;
        }
        agg = new Agg(postGroupFuncs.get(postIdx), postGroupFields.get(postIdx), postGroupOffsets.get(postIdx));
        emittedPostGroupVars.add(valueVar);
        emittedPostGroupAt.add(keyCount + i);
      } else {
        agg = aggregateCall(value, loopVar, letVars, letFields, letOffsets);
        if (agg == null) {
          return;
        }
      }
      if (agg.offset() != 0L && !constMode) {
        // The per-group kernels do not apply offsets; only the single-group route does.
        return;
      }
      funcs[i] = agg.func();
      fields[i] = agg.field();
      offsets[i] = agg.offset();
      outNames[i] = name;
    }
    // Resolve each order-by spec to the index of the emitted entry it sorts on. A key that is
    // NOT emitted would have to be recomputed outside the scan — decline rather than guess.
    final int orderCount = orderVars.size();
    final int[] orderIndexes = new int[orderCount];
    final boolean[] orderAscending = new boolean[orderCount];
    final boolean[] orderEmptyLeastFlags = new boolean[orderCount];
    for (int i = 0; i < orderCount; i++) {
      final QNm orderVar = orderVars.get(i);
      int at = -1;
      final int keyAt = indexOfKeyEntry(returnExpr, keyCount, orderVar);
      if (keyAt >= 0) {
        at = keyAt;
      } else {
        final int postAt = emittedPostGroupVars.indexOf(orderVar);
        if (postAt >= 0) {
          at = emittedPostGroupAt.get(postAt);
        }
      }
      if (at < 0) {
        return;
      }
      orderIndexes[i] = at;
      orderAscending[i] = orderAsc.get(i);
      orderEmptyLeastFlags[i] = orderEmptyLeast.get(i);
    }
    if (constMode) {
      pipeExpr.setProperty(GROUP_AGG_CONST, Boolean.TRUE);
      pipeExpr.setProperty(GROUP_AGG_FUNCS, funcs);
      pipeExpr.setProperty(GROUP_AGG_FIELDS, fields);
      pipeExpr.setProperty(GROUP_AGG_OUT_NAMES, outNames);
      pipeExpr.setProperty(GROUP_AGG_OFFSETS, offsets);
      return;
    }
    pipeExpr.setProperty(GROUP_AGG, Boolean.TRUE);
    pipeExpr.setProperty(GROUP_AGG_GROUP_FIELDS, groupFields);
    pipeExpr.setProperty(GROUP_AGG_KEY_NAMES, keyNames);
    pipeExpr.setProperty(GROUP_AGG_FUNCS, funcs);
    pipeExpr.setProperty(GROUP_AGG_FIELDS, fields);
    pipeExpr.setProperty(GROUP_AGG_OUT_NAMES, outNames);
    if (orderCount > 0) {
      pipeExpr.setProperty(GROUP_AGG_ORDER_INDEXES, orderIndexes);
      pipeExpr.setProperty(GROUP_AGG_ORDER_ASC, orderAscending);
      pipeExpr.setProperty(GROUP_AGG_ORDER_EMPTY_LEAST, orderEmptyLeastFlags);
    }
  }

  /**
   * Index of the KEY entry bound to {@code var}, or {@code -1}. Keys occupy entries 0..keyCount-1.
   */
  private static int indexOfKeyEntry(final AST returnExpr, final int keyCount, final QNm var) {
    for (int i = 0; i < keyCount; i++) {
      final AST value = returnExpr.getChild(i).getChild(1);
      if (value.getType() == XQ.VariableRef && var.equals(value.getValue())) {
        return i;
      }
    }
    return -1;
  }

  /** A servable aggregate: {@code field} is {@code null} for {@code count}; {@code offset} is the
   * constant a shifted operand let added ({@code 0} for a plain deref). */
  private record Agg(String func, String field, long offset) {
  }

  /** A pre-group let's shifted operand {@code $loop.field + k} / {@code k + $loop.field} /
   * {@code $loop.field - k}. */
  private record ShiftedDeref(String field, long offset) {
  }

  /**
   * Parse an aggregate call over the grouped loop variable.
   *
   * <p>
   * Accepts {@code count($loop)} and {@code sum|min|max|avg(<value>)}, where {@code <value>} is
   * either a direct {@code $loop.field} deref or a reference to a PRE-group let that binds such a
   * deref — after the group-by both denote the same per-group multiset, so
   * {@code let $a := $r.amount ... sum($a)} is exactly {@code sum($r.amount)}. A let that binds a
   * SHIFTED deref ({@code $r.f + k}) carries its offset; only the constant-grouping route may
   * consume a non-zero one.
   *
   * @return the aggregate, or {@code null} when the expression is not servable
   */
  private static Agg aggregateCall(final AST call, final QNm loopVar, final List<QNm> letVars,
      final List<String> letFields, final List<Long> letOffsets) {
    if (call == null || call.getType() != XQ.FunctionCall || call.getChildCount() != 1
        || !(call.getValue() instanceof QNm fn)) {
      return null;
    }
    // Built-in aggregates ONLY: a user-defined function whose LOCAL name is sum/min/max/avg/count
    // (say local:sum) must never be served with fn:* semantics. Unprefixed calls resolve to
    // Brackit's JSONiq default-function namespace; fn:* stays the XQuery namespace — both builtins.
    final String ns = fn.getNamespaceURI();
    if (ns != null && !ns.isEmpty() && !Namespaces.FN_NSURI.equals(ns) && !Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
      return null;
    }
    final String func = fn.getLocalName();
    final AST arg = call.getChild(0);
    if ("count".equals(func)) {
      return arg.getType() == XQ.VariableRef && loopVar.equals(arg.getValue())
          ? new Agg(func, null, 0L)
          : null;
    }
    if (!VALUE_FUNCS.contains(func)) {
      return null;
    }
    final String direct = loopVarDerefField(arg, loopVar);
    if (direct != null) {
      return new Agg(func, direct, 0L);
    }
    if (arg.getType() == XQ.VariableRef && arg.getValue() instanceof QNm argVar) {
      final int letIdx = letVars.indexOf(argVar);
      if (letIdx >= 0) {
        return new Agg(func, letFields.get(letIdx), letOffsets.get(letIdx));
      }
    }
    return null;
  }

  /**
   * {@code $loopVar.field + k}, {@code k + $loopVar.field} or {@code $loopVar.field - k} with an
   * INTEGER literal — the shifted-operand shape {@code sum(f+k) = sum(f) + k·presentCount(f)}
   * serves algebraically. Anything else (doubles, nested arithmetic, two derefs) returns
   * {@code null}.
   */
  private static ShiftedDeref shiftedDeref(final AST expr, final QNm loopVar) {
    if (expr == null || expr.getType() != XQ.ArithmeticExpr || expr.getChildCount() != 3) {
      return null;
    }
    final int opType = expr.getChild(0).getType();
    if (opType != XQ.AddOp && opType != XQ.SubtractOp) {
      return null;
    }
    final AST left = expr.getChild(1);
    final AST right = expr.getChild(2);
    final String leftField = loopVarDerefField(left, loopVar);
    if (leftField != null && right.getType() == XQ.Int) {
      final long k = intValue(right);
      if (k == Long.MIN_VALUE) {
        return null;
      }
      return new ShiftedDeref(leftField, opType == XQ.AddOp
          ? k
          : -k);
    }
    if (opType == XQ.AddOp && left.getType() == XQ.Int) {
      // k + $r.f — addition commutes; k - $r.f does NOT reduce to a shift and stays declined.
      final String rightField = loopVarDerefField(right, loopVar);
      final long k = intValue(left);
      if (rightField != null && k != Long.MIN_VALUE) {
        return new ShiftedDeref(rightField, k);
      }
    }
    return null;
  }

  /** Value of an integer literal node, or {@code Long.MIN_VALUE} when it is not usable (that
   * sentinel itself would negate to an overflow, so it declines too). */
  private static long intValue(final AST node) {
    final Object v = node.getValue();
    if (v instanceof Int32 i32) {
      return i32.longValue();
    }
    if (v instanceof Int64 i64 && i64.longValue() != Long.MIN_VALUE) {
      return i64.longValue();
    }
    return Long.MIN_VALUE;
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
    return name instanceof String s
        ? s
        : null;
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
    return binding.getChild(0).getValue() instanceof QNm qnm
        ? qnm
        : null;
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
    return val instanceof String s
        ? s
        : null;
  }
}
