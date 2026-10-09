package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Detection of the CORRELATED index-routed grouping: an outer loop over a small table whose rows
 * supply the inner opener's instants and some of the group keys.
 *
 * <pre>
 *   for $o in OUTER [where p($o)]
 *   for $r in jn:open-bitemporal('db','res', f($o), g($o))
 *   let $k1 := e($o), $k2 := $r.field, $v := $r.a * $r.b
 *   group by $k1, $k2
 *   let $n := count($v), $s := sum($v)
 *   order by $k1, $k2
 *   return {"k1": $k1, "k2": $k2, "n": $n, "s": $s}
 * </pre>
 *
 * The inner loop, its inner lets, the inner group keys and the aggregates are exactly the plain
 * index-routed grouping; the stage builds that SYNTHETIC inner pipe (outer keys and the order-by
 * removed), lets {@link IndexRoutedSourceStage} and {@link GroupAggregateDetectionStage} annotate
 * it, and records beside it what the outer loop contributes: the outer variable, the outer key
 * expressions (any expression over the outer row, evaluated per outer tuple by the interpreter),
 * where every entry of the real record comes from, and the real order-by. The serving expression
 * runs the outer prefix as an ordinary operator chain, serves the inner grouping once per outer
 * tuple, and merges the groups on (outer keys, inner keys).
 *
 * <p>
 * Admission is deliberately narrow: plain loops; selections before the inner loop over the outer
 * row only; every pre-group let over one side only; aggregates merge-able across outer tuples
 * ({@code count}, {@code sum}, {@code min}, {@code max} — an {@code avg} or a distinct count would
 * need the lanes behind the emitted value); and an order-by naming every group key, which the
 * wrapper applies (the merged groups' order is otherwise outer-major first appearance, which the
 * generic pipeline shares only without order exceptions in the projection).
 * </p>
 */
public final class CorrelatedGroupAggregateDetectionStage implements Stage {

  public static final String CORRELATED = "SIRIX_CORR_GROUP";
  /** {@code AST}: the annotated synthetic inner pipe. */
  public static final String INNER_PIPE = "SIRIX_CORR_INNER_PIPE";
  /** {@code AST[]}: the outer key expressions, in outer-key order. */
  public static final String OUTER_KEY_EXPRS = "SIRIX_CORR_OUTER_KEY_EXPRS";
  /** {@code String[]}: the outer keys' record entry names. */
  public static final String OUTER_KEY_NAMES = "SIRIX_CORR_OUTER_KEY_NAMES";
  /**
   * {@code int[]} per real record entry: {@code >= 0} the outer key index, {@code < 0} the synthetic
   * record's entry {@code -(value + 1)} (an inner key or an aggregate).
   */
  public static final String ENTRY_KINDS = "SIRIX_CORR_ENTRY_KINDS";
  /** {@code int[]} / {@code boolean[]} / {@code boolean[]}: the order-by over the real record. */
  public static final String ORDER_INDEXES = "SIRIX_CORR_ORDER_INDEXES";
  public static final String ORDER_ASC = "SIRIX_CORR_ORDER_ASC";
  public static final String ORDER_EMPTY_LEAST = "SIRIX_CORR_ORDER_EMPTY_LEAST";

  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");
  private static final Set<String> MERGEABLE = Set.of("count", "sum", "min", "max");

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    walk(sctx, ast);
    return ast;
  }

  private void walk(final StaticContext sctx, final AST node) {
    if (node == null) {
      return;
    }
    if (node.getType() == XQ.PipeExpr) {
      final String decline = tryAnnotate(sctx, node);
      if (decline != null && DIAG) {
        System.err.println("[corr-decline] " + decline);
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(sctx, node.getChild(i));
    }
  }

  private @Nullable String tryAnnotate(final StaticContext sctx, final AST pipeExpr) {
    if (Boolean.TRUE.equals(pipeExpr.getProperty(GroupAggregateDetectionStage.GROUP_AGG))) {
      return null; // the plain shape, already claimed
    }
    if (pipeExpr.getChildCount() < 1) {
      return "pipe: no children";
    }
    final AST chain = pipeExpr.getChild(0);
    if (chain.getType() != XQ.Start || chain.getChildCount() < 1) {
      return "pipe: chain is not a Start node";
    }
    final AST outerFor = chain.getLastChild();
    if (outerFor == null || outerFor.getType() != XQ.ForBind || !plainForBind(outerFor)) {
      return "outer: not a plain for";
    }
    final QNm outerVar = bindingVarName(outerFor);
    if (outerVar == null) {
      return "outer: loop variable is not a name";
    }
    AST current = outerFor.getLastChild();
    while (current != null && current.getType() == XQ.Selection) {
      if (!onlyReferences(current.getChild(0), Set.of(outerVar), Set.of())) {
        return "outer where: references a variable other than the outer loop var";
      }
      current = current.getLastChild();
    }
    final AST innerFor = current;
    if (innerFor == null || innerFor.getType() != XQ.ForBind || !plainForBind(innerFor)) {
      return "inner: not a plain for";
    }
    final QNm innerVar = bindingVarName(innerFor);
    if (innerVar == null || innerVar.equals(outerVar)) {
      return "inner: loop variable is not a name or shadows the outer";
    }
    // Pre-group lets: each over exactly one side.
    final List<AST> outerLets = new ArrayList<>();
    final List<QNm> outerLetVars = new ArrayList<>();
    final List<AST> innerLets = new ArrayList<>();
    final Set<QNm> innerLetVars = new HashSet<>();
    current = innerFor.getLastChild();
    while (current != null && current.getType() == XQ.LetBind) {
      final QNm letVar = bindingVarName(current);
      if (letVar == null || letVar.equals(outerVar) || letVar.equals(innerVar) || outerLetVars.contains(letVar)
          || innerLetVars.contains(letVar)) {
        return "let: variable shadows a loop var or an earlier binding";
      }
      final AST bound = current.getChild(1);
      if (onlyReferences(bound, Set.of(outerVar), Set.of())) {
        outerLets.add(current);
        outerLetVars.add(letVar);
      } else if (onlyReferences(bound, Set.of(innerVar), innerLetVars)) {
        innerLets.add(current);
        innerLetVars.add(letVar);
      } else {
        return "let: binding reads both sides, or an unknown variable";
      }
      current = current.getLastChild();
    }
    if (current == null || current.getType() != XQ.GroupBy) {
      return "pipeline: no group-by after the pre-group lets";
    }
    final AST groupBy = current;
    final List<QNm> outerKeyVars = new ArrayList<>();
    final List<QNm> innerKeyVars = new ArrayList<>();
    for (int i = 0; i < groupBy.getChildCount(); i++) {
      final AST spec = groupBy.getChild(i);
      if (spec.getType() != XQ.GroupBySpec) {
        continue;
      }
      final AST ref = spec.getChildCount() > 0
          ? spec.getChild(0)
          : null;
      if (ref == null || ref.getType() != XQ.VariableRef || !(ref.getValue() instanceof QNm var)) {
        return "group by: spec is not a bare variable reference";
      }
      if (outerLetVars.contains(var)) {
        if (outerKeyVars.contains(var)) {
          return "group by: duplicate key";
        }
        outerKeyVars.add(var);
      } else if (innerLetVars.contains(var)) {
        if (innerKeyVars.contains(var)) {
          return "group by: duplicate key";
        }
        innerKeyVars.add(var);
      } else {
        return "group by: key is not a pre-group let";
      }
    }
    if (outerKeyVars.isEmpty()) {
      return "group by: no outer key (the plain shape serves this)";
    }
    if (innerKeyVars.isEmpty()) {
      return "group by: no inner key";
    }
    // Every outer let must be a key: an outer value inside an aggregate has no lane.
    for (final QNm outerLet : outerLetVars) {
      if (!outerKeyVars.contains(outerLet)) {
        return "let: outer binding that is not a group key";
      }
    }
    // Post-group lets, then the order-by, then the return.
    final List<AST> postLets = new ArrayList<>();
    final List<QNm> postVars = new ArrayList<>();
    current = groupBy.getLastChild();
    while (current != null && current.getType() == XQ.LetBind) {
      final QNm var = bindingVarName(current);
      if (var == null) {
        return "post-group let: variable is not a name";
      }
      postLets.add(current);
      postVars.add(var);
      current = current.getLastChild();
    }
    if (current == null || current.getType() != XQ.OrderBy) {
      return "order by: absent (the merged groups need a total order)";
    }
    final AST orderBy = current;
    final List<QNm> orderVars = new ArrayList<>();
    final List<Boolean> orderAsc = new ArrayList<>();
    final List<Boolean> orderEmptyLeast = new ArrayList<>();
    for (int i = 0; i < orderBy.getChildCount(); i++) {
      final AST spec = orderBy.getChild(i);
      if (spec.getType() != XQ.OrderBySpec) {
        continue;
      }
      if (spec.getChildCount() < 1 || spec.getChild(0).getType() != XQ.VariableRef
          || !(spec.getChild(0).getValue() instanceof QNm orderVar)) {
        return "order by: key is not a bare variable reference";
      }
      boolean asc = true;
      boolean emptyLeast = true;
      for (int m = 1; m < spec.getChildCount(); m++) {
        final AST modifier = spec.getChild(m);
        if (modifier.getType() == XQ.OrderByKind) {
          asc = modifier.getChild(0).getType() == XQ.ASCENDING;
        } else if (modifier.getType() == XQ.OrderByEmptyMode) {
          emptyLeast = modifier.getChild(0).getType() == XQ.LEAST;
        } else {
          return "order by: unsupported modifier";
        }
      }
      orderVars.add(orderVar);
      orderAsc.add(asc);
      orderEmptyLeast.add(emptyLeast);
    }
    current = orderBy.getLastChild();
    if (current == null || current.getType() != XQ.End || current.getChildCount() < 1) {
      return "pipeline: no return after the order-by";
    }
    final AST returnExpr = current.getChild(0);
    if (returnExpr.getType() != XQ.ObjectConstructor) {
      return "return: not an object constructor";
    }
    // The real record: outer keys, inner keys and aggregate entries in the query's order.
    final int entries = returnExpr.getChildCount();
    final int[] entryKinds = new int[entries];
    final List<AST> syntheticKeyEntries = new ArrayList<>();
    final List<AST> syntheticAggEntries = new ArrayList<>();
    final List<QNm> entryVars = new ArrayList<>(entries);
    final List<AST> outerKeyExprs = new ArrayList<>();
    final List<String> outerKeyNames = new ArrayList<>();
    final Set<QNm> seenOuter = new HashSet<>();
    final Set<QNm> seenInner = new HashSet<>();
    for (int i = 0; i < entries; i++) {
      final AST entry = returnExpr.getChild(i);
      if (entry.getType() != XQ.KeyValueField || entry.getChildCount() != 2 || entry.getChild(0).getType() != XQ.Str) {
        return "return: entry is not a named field";
      }
      final AST value = entry.getChild(1);
      if (value.getType() != XQ.VariableRef || !(value.getValue() instanceof QNm var)) {
        return "return: entry value is not a variable reference";
      }
      entryVars.add(var);
      if (outerKeyVars.contains(var)) {
        if (!seenOuter.add(var)) {
          return "return: outer key emitted twice";
        }
        entryKinds[i] = outerKeyExprs.size();
        outerKeyExprs.add(outerLets.get(outerLetVars.indexOf(var)).getChild(1));
        outerKeyNames.add(entry.getChild(0).getValue().toString());
      } else if (innerKeyVars.contains(var)) {
        if (!seenInner.add(var)) {
          return "return: inner key emitted twice";
        }
        entryKinds[i] = -(syntheticKeyEntries.size() + 1);
        syntheticKeyEntries.add(entry);
      } else if (postVars.contains(var)) {
        entryKinds[i] = Integer.MIN_VALUE; // resolved below, after the key entries are counted
        syntheticAggEntries.add(entry);
      } else {
        return "return: entry is neither a key nor a post-group aggregate";
      }
    }
    if (seenOuter.size() != outerKeyVars.size() || seenInner.size() != innerKeyVars.size()) {
      return "return: record does not echo every group key";
    }
    int aggAt = syntheticKeyEntries.size();
    for (int i = 0; i < entries; i++) {
      if (entryKinds[i] == Integer.MIN_VALUE) {
        entryKinds[i] = -(aggAt + 1);
        aggAt++;
      }
    }
    // Order-by must name every key, resolved to real record positions.
    final int[] orderIndexes = new int[orderVars.size()];
    for (int i = 0; i < orderIndexes.length; i++) {
      final int at = entryVars.indexOf(orderVars.get(i));
      if (at < 0) {
        return "order by: sorts on a value the record does not emit";
      }
      orderIndexes[i] = at;
    }
    for (final QNm key : outerKeyVars) {
      if (!orderVars.contains(key)) {
        return "order by: does not name every group key";
      }
    }
    for (final QNm key : innerKeyVars) {
      if (!orderVars.contains(key)) {
        return "order by: does not name every group key";
      }
    }
    // The synthetic inner pipe: for $r in OPENER, inner lets, group by inner keys, post lets, return.
    final AST synthetic = syntheticInnerPipe(innerFor, innerLets, groupBy, innerKeyVars, postLets, syntheticKeyEntries,
        syntheticAggEntries);
    if (!IndexRoutedSourceStage.annotate(synthetic)) {
      return "inner: source is not an admitted index-routed opener";
    }
    new GroupAggregateDetectionStage().rewrite(sctx, synthetic);
    if (!Boolean.TRUE.equals(synthetic.getProperty(GroupAggregateDetectionStage.GROUP_AGG))) {
      return "inner: the plain grouping declined the synthetic pipe";
    }
    final String[] funcs = (String[]) synthetic.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FUNCS);
    for (final String func : funcs) {
      if (!MERGEABLE.contains(func)) {
        return "aggregate: " + func + " cannot be merged across outer tuples";
      }
    }
    pipeExpr.setProperty(CORRELATED, Boolean.TRUE);
    pipeExpr.setProperty(INNER_PIPE, synthetic);
    pipeExpr.setProperty(OUTER_KEY_EXPRS, outerKeyExprs.toArray(new AST[0]));
    pipeExpr.setProperty(OUTER_KEY_NAMES, outerKeyNames.toArray(new String[0]));
    pipeExpr.setProperty(ENTRY_KINDS, entryKinds);
    pipeExpr.setProperty(ORDER_INDEXES, orderIndexes);
    final boolean[] asc = new boolean[orderIndexes.length];
    final boolean[] emptyLeast = new boolean[orderIndexes.length];
    for (int i = 0; i < orderIndexes.length; i++) {
      asc[i] = orderAsc.get(i);
      emptyLeast[i] = orderEmptyLeast.get(i);
    }
    pipeExpr.setProperty(ORDER_ASC, asc);
    pipeExpr.setProperty(ORDER_EMPTY_LEAST, emptyLeast);
    return null;
  }

  /**
   * {@code PipeExpr(Start(ForBind(inner, lets..., GroupBy(inner specs, postLets..., End(record)))))}.
   */
  private static AST syntheticInnerPipe(final AST innerFor, final List<AST> innerLets, final AST groupBy,
      final List<QNm> innerKeyVars, final List<AST> postLets, final List<AST> keyEntries, final List<AST> aggEntries) {
    final AST record = new AST(XQ.ObjectConstructor);
    for (final AST entry : keyEntries) {
      record.addChild(entry.copyTree());
    }
    for (final AST entry : aggEntries) {
      record.addChild(entry.copyTree());
    }
    final AST end = new AST(XQ.End);
    end.addChild(record);
    AST tail = end;
    for (int i = postLets.size() - 1; i >= 0; i--) {
      final AST let = new AST(XQ.LetBind);
      let.addChild(postLets.get(i).getChild(0).copyTree());
      let.addChild(postLets.get(i).getChild(1).copyTree());
      let.addChild(tail);
      tail = let;
    }
    final AST group = new AST(XQ.GroupBy);
    for (int i = 0; i < groupBy.getChildCount(); i++) {
      final AST spec = groupBy.getChild(i);
      if (spec.getType() == XQ.GroupBySpec && spec.getChildCount() > 0
          && innerKeyVars.contains(spec.getChild(0).getValue())) {
        group.addChild(spec.copyTree());
      }
    }
    group.addChild(tail);
    tail = group;
    for (int i = innerLets.size() - 1; i >= 0; i--) {
      final AST let = new AST(XQ.LetBind);
      let.addChild(innerLets.get(i).getChild(0).copyTree());
      let.addChild(innerLets.get(i).getChild(1).copyTree());
      let.addChild(tail);
      tail = let;
    }
    final AST forBind = new AST(XQ.ForBind);
    forBind.addChild(innerFor.getChild(0).copyTree());
    forBind.addChild(innerFor.getChild(1).copyTree());
    forBind.addChild(tail);
    final AST start = new AST(XQ.Start);
    start.addChild(forBind);
    final AST pipe = new AST(XQ.PipeExpr);
    pipe.addChild(start);
    return pipe;
  }

  private static boolean plainForBind(final AST forBind) {
    return forBind.getChildCount() == 3 && forBind.getChild(0).getType() == XQ.TypedVariableBinding
        && forBind.getChild(1).getType() != XQ.AllowingEmpty
        && forBind.getChild(1).getType() != XQ.TypedVariableBinding;
  }

  private static @Nullable QNm bindingVarName(final AST bindNode) {
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

  /**
   * Every variable the subtree references is in {@code allowed} or {@code alsoAllowed}, and at least
   * one is: a subtree over no variable at all (a literal) counts as neither side.
   */
  private static boolean onlyReferences(final AST node, final Set<?> allowed, final Set<QNm> alsoAllowed) {
    final boolean[] any = new boolean[1];
    return referencesOnly(node, allowed, alsoAllowed, any) && any[0];
  }

  private static boolean referencesOnly(final AST node, final Set<?> allowed, final Set<QNm> alsoAllowed,
      final boolean[] any) {
    if (node == null) {
      return true;
    }
    if (node.getType() == XQ.VariableRef) {
      final Object var = node.getValue();
      if (!allowed.contains(var) && !alsoAllowed.contains(var)) {
        // A prolog variable (declared outside every pipeline) is allowed on either side; a variable
        // bound inside the query that is neither side's is not. Prolog variables carry no pipeline
        // binding, which the stage cannot tell apart here — the translator's compile resolves both,
        // so only the admitted-side rule is enforced for pipeline-bound names.
        return false;
      }
      any[0] = true;
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (!referencesOnly(node.getChild(i), allowed, alsoAllowed, any)) {
        return false;
      }
    }
    return true;
  }
}
