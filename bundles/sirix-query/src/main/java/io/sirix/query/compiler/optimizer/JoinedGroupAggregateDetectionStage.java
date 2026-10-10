package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.Int32;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.sirix.query.compiler.optimizer.ComputedProgram.Program;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.Cmp;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Detection of the COLUMN-SIDE equality join with a grouped aggregate over the joined pairs:
 *
 * <pre>
 *   for $c in SRC1 for $s in SRC2 where $c.f eq $s.g
 *   let $k1 := $s.a, $k2 := $c.b, $v := $c.x * $c.y
 *   group by $k1, $k2
 *   let $n := count($v), $t := sum($v)
 *   order by $k1, $k2
 *   return {"k1": $k1, "k2": $k2, "n": $n, "t": $t}
 * </pre>
 *
 * which Brackit's join recognition has turned into a {@code Join} node over two single-loop
 * branches (SH1 Q9). Each branch's source is an index-routed opener or a literal document; the join
 * compares one field of each side for equality. The serving expression reads both sides' masked
 * columns, hashes the smaller side on its join values, probes with the other, groups the pairs on
 * keys from either side and folds {@code count}, {@code sum}, {@code min} and {@code max} over
 * fields or paired {@code +,-,*} programs, including distinct field counts.
 *
 * <p>
 * Grouped output requires an order-by naming every key. Both grouped and row output admit paired
 * numeric and explicit timestamp comparisons; range sources provide exact record-key masks before
 * the join. Unsupported comparisons and unsafe runtime coverage retain the generic pipeline.
 * </p>
 */
public final class JoinedGroupAggregateDetectionStage implements Stage {

  public static final String JOIN_GROUP = "SIRIX_JOIN_GROUP";
  public static final String ROW_JOIN = "SIRIX_ROW_JOIN";
  public static final String RESIDUAL_SIDES = "SIRIX_JOIN_RESIDUAL_SIDES";
  public static final String RESIDUAL_FIELDS = "SIRIX_JOIN_RESIDUAL_FIELDS";
  public static final String RESIDUAL_OPS = "SIRIX_JOIN_RESIDUAL_OPS";
  public static final String RESIDUAL_CODE = "SIRIX_JOIN_RESIDUAL_CODE";
  public static final String SIDE_SOURCE_EXPRS = "SIRIX_JOIN_SIDE_SOURCE_EXPRS";
  /** Per side ({@code 0} left, {@code 1} right): literal database and resource. */
  public static final String SIDE_DATABASES = "SIRIX_JOIN_SIDE_DATABASES";
  public static final String SIDE_RESOURCES = "SIRIX_JOIN_SIDE_RESOURCES";
  /** Per side: {@code AST} instants of a routed opener ({@code null} entries for a document). */
  public static final String SIDE_TX_TIMES = "SIRIX_JOIN_SIDE_TX_TIMES";
  public static final String SIDE_VALID_TIMES = "SIRIX_JOIN_SIDE_VALID_TIMES";
  /**
   * Per side: the literal revision of a document source ({@code -1} latest, ignored for an opener).
   */
  public static final String SIDE_REVISIONS = "SIRIX_JOIN_SIDE_REVISIONS";
  /** Per side: the join field. */
  public static final String JOIN_FIELDS = "SIRIX_JOIN_FIELDS";
  /** Per group key: side and field. */
  public static final String KEY_SIDES = "SIRIX_JOIN_KEY_SIDES";
  public static final String KEY_FIELDS = "SIRIX_JOIN_KEY_FIELDS";
  /**
   * Per aggregate: function, side ({@code -1} = a pair count), field ({@code null} for a pair count,
   * {@code prog:<i>} for a program).
   */
  public static final String AGG_FUNCS = "SIRIX_JOIN_AGG_FUNCS";
  public static final String AGG_SIDES = "SIRIX_JOIN_AGG_SIDES";
  public static final String AGG_FIELDS = "SIRIX_JOIN_AGG_FIELDS";
  /** Per program: side, operand fields, code, constants. */
  public static final String PROG_SIDES = "SIRIX_JOIN_PROG_SIDES";
  public static final String PROG_FIELDS = "SIRIX_JOIN_PROG_FIELDS";
  public static final String PROG_CODE = "SIRIX_JOIN_PROG_CODE";
  public static final String PROG_CONSTS = "SIRIX_JOIN_PROG_CONSTS";
  /** Per real record entry: {@code >= 0} key index, {@code < 0} aggregate {@code -(value + 1)}. */
  public static final String ENTRY_KINDS = "SIRIX_JOIN_ENTRY_KINDS";
  public static final String ORDER_INDEXES = "SIRIX_JOIN_ORDER_INDEXES";
  public static final String ORDER_ASC = "SIRIX_JOIN_ORDER_ASC";
  public static final String ORDER_EMPTY_LEAST = "SIRIX_JOIN_ORDER_EMPTY_LEAST";

  private static final String COMPUTED_PREFIX = "prog:";
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");
  private static final Set<String> FUNCS = Set.of("count", "sum", "min", "max");

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
      final String decline = tryAnnotate(node);
      if (decline != null && DIAG) {
        System.err.println("[join-decline] " + decline);
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(node.getChild(i));
    }
  }

  /** A pre-group let: a plain field of one side, or a program over one side's fields. */
  private record SideLet(QNm var, int side, String field) {
  }

  private @Nullable String tryAnnotate(final AST pipeExpr) {
    if (pipeExpr.getChildCount() < 1) {
      return "pipe: no children";
    }
    final AST chain = pipeExpr.getChild(0);
    if (chain.getType() != XQ.Start || chain.getChildCount() != 1 || chain.getChild(0).getType() != XQ.Join) {
      return "pipe: not a join at the chain head";
    }
    final AST join = chain.getChild(0);
    if (join.getChildCount() != 4 || !(join.getProperty("cmp") instanceof Cmp cmp) || cmp != Cmp.eq) {
      return "join: not an equality join";
    }
    final String[] databases = new String[2];
    final String[] resources = new String[2];
    final AST[] txTimes = new AST[2];
    final AST[] validTimes = new AST[2];
    final AST[] indexed = new AST[2];
    final int[] revisions = new int[2];
    final String[] joinFields = new String[2];
    final QNm[] vars = new QNm[2];
    for (int side = 0; side < 2; side++) {
      final String decline =
          joinBranch(join, side, vars, databases, resources, txTimes, validTimes, revisions, joinFields, indexed);
      if (decline != null) {
        return decline;
      }
    }
    if (vars[0].equals(vars[1])) {
      return "join: both loops bind the same name";
    }
    final Set<AST> ranges = new HashSet<>();
    final List<AST> comparisons = new ArrayList<>();
    selections(join.getChild(2).getChild(0), comparisons);
    selections(join.getChild(3), comparisons);
    for (int side = 0; side < 2; side++) {
      final AST source = join.getChild(side).getChild(0).getChild(1);
      final AST range = rangeSource(source, comparisons, vars[side], vars, ranges);
      if (range != null) {
        indexed[side] = range;
        validTimes[side] = range.getChild(1);
      } else if (validTimes[side] == null && indexed[side] != null) {
        return "join: dynamic document requires an exact range source";
      }
    }
    final String rowDecline = rowJoin(pipeExpr, join, vars, databases, resources, txTimes, validTimes, revisions,
        joinFields, indexed, ranges);
    if (rowDecline == null) {
      return null;
    }
    return groupedJoin(pipeExpr, join, vars, databases, resources, txTimes, validTimes, revisions, joinFields, indexed,
        ranges);
  }

  private static @Nullable String rowJoin(final AST pipe, final AST join, final QNm[] vars, final String[] databases,
      final String[] resources, final AST[] txTimes, final AST[] validTimes, final int[] revisions,
      final String[] joinFields, final AST[] indexed, final Set<AST> ranges) {
    if (validTimes[0] == null || validTimes[1] == null) {
      return "row join: both sources must be routed";
    }
    final List<Integer> residualSides = new ArrayList<>();
    final List<String> residualFields = new ArrayList<>();
    final List<Integer> residualOps = new ArrayList<>();
    final List<Integer> residualCode = new ArrayList<>();
    final AST post = join.getChild(2);
    if (post.getType() != XQ.Start || post.getChildCount() != 1) {
      return "row join: post chain is not a Start";
    }
    AST tail = post.getChild(0);
    tail = residualSelections(tail, vars, residualSides, residualFields, residualOps, residualCode, ranges);
    if (tail == null) {
      return "row join: unsupported residual";
    }
    if (tail.getType() != XQ.End) {
      return "row join: unsupported post chain";
    }
    AST current = join.getChild(3);
    current = residualSelections(current, vars, residualSides, residualFields, residualOps, residualCode, ranges);
    if (current == null) {
      return "row join: unsupported residual";
    }
    if (!orderedRowRecord(current)) {
      return "row join: requires ordered row records";
    }
    final AST record = current.getLastChild().getChild(0);
    final int count = record.getChildCount();
    final int[] keySides = new int[count];
    final String[] keyFields = new String[count];
    final int[] entryKinds = new int[count];
    final Set<String> names = new HashSet<>();
    final String entryDecline = rowEntries(record, count, vars, keySides, keyFields, entryKinds, names);
    if (entryDecline != null) {
      return entryDecline;
    }
    final List<Integer> orderIndexes = new ArrayList<>();
    final List<Boolean> orderAsc = new ArrayList<>();
    final List<Boolean> emptyLeast = new ArrayList<>();
    final String orderDecline = rowOrder(current, vars, count, keyFields, keySides, orderIndexes, orderAsc, emptyLeast);
    if (orderDecline != null) {
      return orderDecline;
    }
    if (orderIndexes.isEmpty()) {
      return "row join: no ordering keys";
    }
    pipe.setProperty(JOIN_GROUP, true);
    pipe.setProperty(ROW_JOIN, true);
    pipe.setProperty(SIDE_DATABASES, databases);
    pipe.setProperty(SIDE_RESOURCES, resources);
    pipe.setProperty(SIDE_TX_TIMES, txTimes);
    pipe.setProperty(SIDE_VALID_TIMES, validTimes);
    pipe.setProperty(SIDE_SOURCE_EXPRS, indexed);
    pipe.setProperty(SIDE_REVISIONS, revisions);
    pipe.setProperty(JOIN_FIELDS, joinFields);
    pipe.setProperty(KEY_SIDES, keySides);
    pipe.setProperty(KEY_FIELDS, keyFields);
    pipe.setProperty(AGG_FUNCS, new String[0]);
    pipe.setProperty(AGG_SIDES, new int[0]);
    pipe.setProperty(AGG_FIELDS, new String[0]);
    pipe.setProperty(PROG_SIDES, new int[0]);
    pipe.setProperty(PROG_FIELDS, new String[0][]);
    pipe.setProperty(PROG_CODE, new int[0][]);
    pipe.setProperty(PROG_CONSTS, new long[0][]);
    pipe.setProperty(ENTRY_KINDS, entryKinds);
    pipe.setProperty(ORDER_INDEXES, orderIndexes.stream().mapToInt(Integer::intValue).toArray());
    final boolean[] asc = new boolean[orderAsc.size()];
    final boolean[] least = new boolean[asc.length];
    for (int i = 0; i < asc.length; i++) {
      asc[i] = orderAsc.get(i);
      least[i] = emptyLeast.get(i);
    }
    final int[] ops = residualOps.stream().mapToInt(Integer::intValue).toArray();
    pipe.setProperty(ORDER_ASC, asc);
    pipe.setProperty(ORDER_EMPTY_LEAST, least);
    pipe.setProperty(RESIDUAL_SIDES, residualSides.stream().mapToInt(Integer::intValue).toArray());
    pipe.setProperty(RESIDUAL_FIELDS, residualFields.toArray(new String[0]));
    pipe.setProperty(RESIDUAL_OPS, ops);
    pipe.setProperty(RESIDUAL_CODE, residualCode.stream().mapToInt(Integer::intValue).toArray());
    return null;
  }

  private static boolean residual(final AST expression, final QNm[] vars, final List<Integer> sides,
      final List<String> fields, final List<Integer> ops, final List<Integer> code, final Set<AST> ranges) {
    AST node = expression;
    while (node.getType() == XQ.ParenthesizedExpr && node.getChildCount() == 1) {
      node = node.getChild(0);
    }
    if (ranges.contains(node)) {
      return true;
    }
    if (booleanResidual(node)) {
      final int before = code.size();
      if (!residual(node.getChild(0), vars, sides, fields, ops, code, ranges)) {
        return false;
      }
      final int middle = code.size();
      if (!residual(node.getChild(1), vars, sides, fields, ops, code, ranges)) {
        return false;
      }
      if (middle > before && code.size() > middle) {
        code.add(node.getType() == XQ.AndExpr
            ? -1
            : -2);
      }
      return true;
    }
    if (node.getType() != XQ.ComparisonExpr || node.getChildCount() != 3) {
      return false;
    }
    final int op = node.getChild(0).getType();
    final int compare = comparisonOpcode(op);
    if (compare < 0) {
      return false;
    }
    final int count = ops.size();
    for (int i = 1; i <= 2; i++) {
      final AST operand = node.getChild(i);
      final boolean timestamp = dateTimeCast(operand);
      final AST deref = timestamp
          ? operand.getChild(0)
          : operand;
      final int side = ComputedProgram.loopVarDerefField(deref, vars[0]) == null
          ? 1
          : 0;
      final String field = ComputedProgram.loopVarDerefField(deref, vars[side]);
      if (field == null) {
        return false;
      }
      sides.add(side);
      fields.add((timestamp
          ? "timestamp:"
          : "number:") + field);
    }
    if (dateTimeCast(node.getChild(1)) != dateTimeCast(node.getChild(2))) {
      return false;
    }
    ops.add(compare);
    code.add(count);
    return true;
  }

  /** Classify one branch's source: an opener (routed) or a literal document; else a decline. */
  private static @Nullable String source(final AST source, final int side, final String[] databases,
      final String[] resources, final AST[] txTimes, final AST[] validTimes, final int[] revisions,
      final AST[] indexed) {
    final IndexRoutedSourceStage.Source routed = IndexRoutedSourceStage.source(source);
    if (routed != null) {
      databases[side] = routed.database();
      resources[side] = routed.resource();
      txTimes[side] = routed.txTime();
      validTimes[side] = routed.validTime();
      indexed[side] = routed.indexed();
      return null;
    }
    AST call = source;
    boolean members = false;
    if (arrayMembers(call)) {
      // jn:doc('db','res')[]: the array members, which the openers yield directly.
      members = true;
      call = call.getChild(0);
    }
    if (call.getType() != XQ.FunctionCall || !(call.getValue() instanceof QNm fn)
        || !JSONFun.JSON_NSURI.equals(fn.getNamespaceURI())) {
      return "join: source is not a JSON function";
    }
    final String local = fn.getLocalName();
    if (("doc".equals(local) || "open".equals(local)) && members
        && (call.getChildCount() == 2 || call.getChildCount() == 3)) {
      databases[side] = stringLiteral(call.getChild(0));
      resources[side] = stringLiteral(call.getChild(1));
      if (databases[side] == null || resources[side] == null) {
        return "join: dynamic database or resource";
      }
      final String decline = documentRevision(call, side, revisions);
      if (decline != null) {
        indexed[side] = call;
        revisions[side] = -1;
      }
      return null;
    }
    return "join: source is neither an opener nor a literal document";
  }

  private static boolean builtin(final QNm fn) {
    final String ns = fn.getNamespaceURI();
    return ns == null || ns.isEmpty() || Namespaces.FN_NSURI.equals(ns) || Namespaces.DEFAULT_FN_NSURI.equals(ns);
  }

  private static int indexOf(final List<SideLet> lets, final QNm var) {
    for (int i = 0; i < lets.size(); i++) {
      if (var.equals(lets.get(i).var())) {
        return i;
      }
    }
    return -1;
  }

  private static @Nullable QNm bindingVarName(final AST bindNode) {
    if (bindNode.getChildCount() < 1 || bindNode.getChild(0).getChildCount() < 1) {
      return null;
    }
    return bindNode.getChild(0).getChild(0).getValue() instanceof QNm qnm
        ? qnm
        : null;
  }

  private static @Nullable String stringLiteral(final AST node) {
    if (node == null || node.getType() != XQ.Str) {
      return null;
    }
    final Object value = node.getValue();
    if (value instanceof Str str) {
      return str.stringValue();
    }
    return value instanceof String s
        ? s
        : null;
  }

  private static @Nullable String joinBranch(final AST join, final int side, final QNm[] vars, final String[] databases,
      final String[] resources, final AST[] txTimes, final AST[] validTimes, final int[] revisions,
      final String[] joinFields, final AST[] indexed) {
    final AST branch = join.getChild(side);
    if (branch.getType() != XQ.Start || branch.getChildCount() != 1) {
      return "join: branch is not a Start";
    }
    final AST forBind = branch.getChild(0);
    if (forBind.getType() != XQ.ForBind || forBind.getChildCount() != 3
        || forBind.getChild(0).getType() != XQ.TypedVariableBinding || forBind.getChild(1).getType() == XQ.AllowingEmpty
        || forBind.getChild(1).getType() == XQ.TypedVariableBinding || forBind.getChild(2).getType() != XQ.End
        || forBind.getChild(2).getChildCount() != 1) {
      return "join: branch is not a single plain for";
    }
    vars[side] = bindingVarName(forBind);
    if (vars[side] == null) {
      return "join: loop variable is not a name";
    }
    final String sourceDecline =
        source(forBind.getChild(1), side, databases, resources, txTimes, validTimes, revisions, indexed);
    if (sourceDecline != null) {
      return sourceDecline;
    }
    joinFields[side] = ComputedProgram.loopVarDerefField(forBind.getChild(2).getChild(0), vars[side]);
    if (joinFields[side] == null) {
      return "join: key is not a direct field of the loop var";
    }
    return null;
  }

  private static @Nullable String groupedJoin(final AST pipeExpr, final AST join, final QNm[] vars,
      final String[] databases, final String[] resources, final AST[] txTimes, final AST[] validTimes,
      final int[] revisions, final String[] joinFields, final AST[] indexed, final Set<AST> ranges) {
    final AST post = join.getChild(2);
    if (post.getType() != XQ.Start || post.getChildCount() != 1) {
      return "join: post-join operators";
    }
    final List<Integer> residualSides = new ArrayList<>();
    final List<String> residualFields = new ArrayList<>();
    final List<Integer> residualOps = new ArrayList<>();
    final List<Integer> residualCode = new ArrayList<>();
    final AST postEnd =
        residualSelections(post.getChild(0), vars, residualSides, residualFields, residualOps, residualCode, ranges);
    if (postEnd == null || postEnd.getType() != XQ.End) {
      return "join: unsupported grouped residual";
    }
    // The continuation: pre-group lets, group by, post-group lets, order by, return.
    final List<SideLet> lets = new ArrayList<>();
    final List<Integer> progSides = new ArrayList<>();
    final List<String[]> progFields = new ArrayList<>();
    final List<int[]> progCode = new ArrayList<>();
    final List<long[]> progConsts = new ArrayList<>();
    AST current =
        residualSelections(join.getChild(3), vars, residualSides, residualFields, residualOps, residualCode, ranges);
    if (current == null) {
      return "join: unsupported grouped residual";
    }
    while (current != null && current.getType() == XQ.LetBind) {
      final String decline = preGroupLet(current, vars, lets, progSides, progFields, progCode, progConsts);
      if (decline != null) {
        return decline;
      }
      current = current.getLastChild();
    }
    if (current == null || current.getType() != XQ.GroupBy) {
      return "pipeline: no group-by";
    }
    final List<QNm> keyVars = new ArrayList<>();
    final String keyDecline = groupingKeys(current, lets, keyVars);
    if (keyDecline != null) {
      return keyDecline;
    }
    final String decline = groupedAggregates(pipeExpr, vars, databases, resources, txTimes, validTimes, revisions,
        joinFields, indexed, current, lets, keyVars, progSides, progFields, progCode, progConsts);
    if (decline == null) {
      pipeExpr.setProperty(RESIDUAL_SIDES, residualSides.stream().mapToInt(Integer::intValue).toArray());
      pipeExpr.setProperty(RESIDUAL_FIELDS, residualFields.toArray(new String[0]));
      pipeExpr.setProperty(RESIDUAL_OPS, residualOps.stream().mapToInt(Integer::intValue).toArray());
      pipeExpr.setProperty(RESIDUAL_CODE, residualCode.stream().mapToInt(Integer::intValue).toArray());
    }
    return decline;
  }

  private static @Nullable String preGroupLet(final AST current, final QNm[] vars, final List<SideLet> lets,
      final List<Integer> progSides, final List<String[]> progFields, final List<int[]> progCode,
      final List<long[]> progConsts) {
    final QNm letVar = bindingVarName(current);
    if (letVar == null || letVar.equals(vars[0]) || letVar.equals(vars[1]) || indexOf(lets, letVar) >= 0) {
      return "let: variable shadows a loop var or an earlier binding";
    }
    AST bound = current.getChild(1);
    while (bound.getType() == XQ.ParenthesizedExpr && bound.getChildCount() == 1) {
      bound = bound.getChild(0);
    }
    boolean admitted = false;
    for (int side = 0; side < 2 && !admitted; side++) {
      final String field = ComputedProgram.loopVarDerefField(bound, vars[side]);
      if (field != null) {
        lets.add(new SideLet(letVar, side, field));
        admitted = true;
      } else if (bound.getType() == XQ.ArithmeticExpr) {
        final List<String> fields = new ArrayList<>(4);
        final Program program = ComputedProgram.build(bound, vars[side], fields);
        if (program != null && !fields.isEmpty()) {
          lets.add(new SideLet(letVar, side, COMPUTED_PREFIX + progSides.size()));
          progSides.add(side);
          progFields.add(fields.toArray(new String[0]));
          progCode.add(program.code());
          progConsts.add(program.consts());
          admitted = true;
        }
      }
    }
    if (!admitted && bound.getType() == XQ.ArithmeticExpr) {
      final List<String> fields = new ArrayList<>(4);
      final Program program = pairedProgram(bound, vars, fields);
      if (program != null && !fields.isEmpty()) {
        lets.add(new SideLet(letVar, -1, COMPUTED_PREFIX + progSides.size()));
        progSides.add(-1);
        progFields.add(fields.toArray(new String[0]));
        progCode.add(program.code());
        progConsts.add(program.consts());
        admitted = true;
      }
    }
    if (!admitted) {
      return "let: not a field or a supported paired program";
    }
    return null;
  }

  private static @Nullable String groupingKeys(final AST current, final List<SideLet> lets, final List<QNm> keyVars) {
    for (int i = 0; i < current.getChildCount(); i++) {
      final AST spec = current.getChild(i);
      if (spec.getType() != XQ.GroupBySpec) {
        continue;
      }
      if (spec.getChildCount() < 1 || spec.getChild(0).getType() != XQ.VariableRef
          || !(spec.getChild(0).getValue() instanceof QNm var)) {
        return "group by: spec is not a bare variable reference";
      }
      final int at = indexOf(lets, var);
      if (at < 0 || lets.get(at).field().startsWith(COMPUTED_PREFIX) || keyVars.contains(var)) {
        return "group by: key is not a plain pre-group field, or duplicated";
      }
      keyVars.add(var);
    }
    if (keyVars.isEmpty()) {
      return "group by: no key";
    }
    if (keyVars.size() > Long.SIZE) {
      return "group by: too many keys for the missing-value mask";
    }
    return null;
  }

  private static @Nullable String groupedAggregates(final AST pipeExpr, final QNm[] vars, final String[] databases,
      final String[] resources, final AST[] txTimes, final AST[] validTimes, final int[] revisions,
      final String[] joinFields, final AST[] indexed, final AST group, final List<SideLet> lets,
      final List<QNm> keyVars, final List<Integer> progSides, final List<String[]> progFields,
      final List<int[]> progCode, final List<long[]> progConsts) {
    AST current = group;
    final List<QNm> postVars = new ArrayList<>();
    final List<String> postFuncs = new ArrayList<>();
    final List<Integer> postSides = new ArrayList<>();
    final List<String> postFields = new ArrayList<>();
    current = current.getLastChild();
    while (current != null && current.getType() == XQ.LetBind) {
      final String decline = postGroupLet(current, vars, lets, keyVars, postVars, postFuncs, postSides, postFields);
      if (decline != null) {
        return decline;
      }
      current = current.getLastChild();
    }
    if (current == null || current.getType() != XQ.OrderBy) {
      return "order by: absent";
    }
    final List<QNm> orderVars = new ArrayList<>();
    final List<Boolean> orderAsc = new ArrayList<>();
    final List<Boolean> orderEmptyLeast = new ArrayList<>();
    final String orderDecline = groupedOrder(current, orderVars, orderAsc, orderEmptyLeast);
    if (orderDecline != null) {
      return orderDecline;
    }
    return groupedReturn(pipeExpr, databases, resources, txTimes, validTimes, revisions, joinFields, indexed, current,
        lets, keyVars, postVars, postFuncs, postSides, postFields, orderVars, orderAsc, orderEmptyLeast, progSides,
        progFields, progCode, progConsts);
  }

  private static @Nullable String groupedOrder(final AST current, final List<QNm> orderVars,
      final List<Boolean> orderAsc, final List<Boolean> orderEmptyLeast) {
    for (int i = 0; i < current.getChildCount(); i++) {
      final AST spec = current.getChild(i);
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
    return null;
  }

  private static @Nullable String groupedReturn(final AST pipeExpr, final String[] databases, final String[] resources,
      final AST[] txTimes, final AST[] validTimes, final int[] revisions, final String[] joinFields,
      final AST[] indexed, final AST order, final List<SideLet> lets, final List<QNm> keyVars, final List<QNm> postVars,
      final List<String> postFuncs, final List<Integer> postSides, final List<String> postFields,
      final List<QNm> orderVars, final List<Boolean> orderAsc, final List<Boolean> orderEmptyLeast,
      final List<Integer> progSides, final List<String[]> progFields, final List<int[]> progCode,
      final List<long[]> progConsts) {
    AST current = order;
    current = current.getLastChild();
    if (current == null || current.getType() != XQ.End || current.getChildCount() != 1
        || current.getChild(0).getType() != XQ.ObjectConstructor) {
      return "return: not an object constructor";
    }
    final AST record = current.getChild(0);
    final int entries = record.getChildCount();
    final int[] entryKinds = new int[entries];
    final List<QNm> entryVars = new ArrayList<>(entries);
    final List<Integer> keySides = new ArrayList<>();
    final List<String> keyFields = new ArrayList<>();
    final List<Integer> aggAt = new ArrayList<>();
    final Set<QNm> seenKeys = new HashSet<>();
    final String entryDecline = groupedEntries(record, entries, entryKinds, entryVars, lets, keyVars, postVars,
        keySides, keyFields, aggAt, seenKeys);
    if (entryDecline != null) {
      return entryDecline;
    }
    if (seenKeys.size() != keyVars.size()) {
      return "return: record does not echo every key";
    }
    final int[] orderIndexes = new int[orderVars.size()];
    for (int i = 0; i < orderIndexes.length; i++) {
      final int at = entryVars.indexOf(orderVars.get(i));
      if (at < 0) {
        return "order by: sorts on a value the record does not emit";
      }
      orderIndexes[i] = at;
    }
    for (final QNm key : keyVars) {
      if (!orderVars.contains(key)) {
        return "order by: does not name every key";
      }
    }
    pipeExpr.setProperty(JOIN_GROUP, true);
    pipeExpr.setProperty(SIDE_DATABASES, databases);
    pipeExpr.setProperty(SIDE_RESOURCES, resources);
    pipeExpr.setProperty(SIDE_TX_TIMES, txTimes);
    pipeExpr.setProperty(SIDE_VALID_TIMES, validTimes);
    pipeExpr.setProperty(SIDE_SOURCE_EXPRS, indexed);
    pipeExpr.setProperty(SIDE_REVISIONS, revisions);
    pipeExpr.setProperty(JOIN_FIELDS, joinFields);
    pipeExpr.setProperty(KEY_SIDES, keySides.stream().mapToInt(Integer::intValue).toArray());
    pipeExpr.setProperty(KEY_FIELDS, keyFields.toArray(new String[0]));
    final String[] aggFuncs = new String[aggAt.size()];
    final int[] aggSides = new int[aggAt.size()];
    final String[] aggFields = new String[aggAt.size()];
    for (int i = 0; i < aggAt.size(); i++) {
      aggFuncs[i] = postFuncs.get(aggAt.get(i));
      aggSides[i] = postSides.get(aggAt.get(i));
      aggFields[i] = postFields.get(aggAt.get(i));
    }
    pipeExpr.setProperty(AGG_FUNCS, aggFuncs);
    pipeExpr.setProperty(AGG_SIDES, aggSides);
    pipeExpr.setProperty(AGG_FIELDS, aggFields);
    pipeExpr.setProperty(PROG_SIDES, progSides.stream().mapToInt(Integer::intValue).toArray());
    pipeExpr.setProperty(PROG_FIELDS, progFields.toArray(new String[0][]));
    pipeExpr.setProperty(PROG_CODE, progCode.toArray(new int[0][]));
    pipeExpr.setProperty(PROG_CONSTS, progConsts.toArray(new long[0][]));
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

  private static @Nullable String postGroupLet(final AST current, final QNm[] vars, final List<SideLet> lets,
      final List<QNm> keyVars, final List<QNm> postVars, final List<String> postFuncs, final List<Integer> postSides,
      final List<String> postFields) {
    final QNm var = bindingVarName(current);
    if (var == null || indexOf(lets, var) >= 0 || postVars.contains(var)) {
      return "post-group let: shadows an earlier binding";
    }
    final AST call = current.getChild(1);
    if (!unaryFunction(call) || !(call.getValue() instanceof QNm fn) || !FUNCS.contains(fn.getLocalName())
        || !builtin(fn)) {
      return "post-group let: not count/sum/min/max";
    }
    AST arg = call.getChild(0);
    final boolean distinct =
        "count".equals(fn.getLocalName()) && unaryFunction(arg) && arg.getValue() instanceof QNm distinctFn
            && "distinct-values".equals(distinctFn.getLocalName()) && builtin(distinctFn);
    if (distinct) {
      arg = arg.getChild(0);
    }
    if (arg.getType() != XQ.VariableRef || !(arg.getValue() instanceof QNm argVar)) {
      for (int side = 0; side < 2; side++) {
        final String field = ComputedProgram.loopVarDerefField(arg, vars[side]);
        if (field != null) {
          postSides.add(side);
          postFields.add(field);
          postVars.add(var);
          postFuncs.add(distinct
              ? "count-distinct"
              : fn.getLocalName());
          return null;
        }
      }
      return "post-group let: argument is neither a variable nor a direct field";
    }
    if (keyVars.contains(argVar)) {
      return "post-group let: grouping variables are scalar values";
    }
    final String func = distinct
        ? "count-distinct"
        : fn.getLocalName();
    if (argVar.equals(vars[0]) || argVar.equals(vars[1])) {
      if (!"count".equals(func)) {
        return "post-group let: value function over a loop var";
      }
      postSides.add(-1);
      postFields.add(null);
    } else {
      final int at = indexOf(lets, argVar);
      if (at < 0) {
        return "post-group let: argument is not a pre-group let";
      }
      postSides.add(lets.get(at).side());
      postFields.add(lets.get(at).field());
    }
    postVars.add(var);
    postFuncs.add(func);
    return null;
  }

  private static @Nullable String groupedEntries(final AST record, final int entries, final int[] entryKinds,
      final List<QNm> entryVars, final List<SideLet> lets, final List<QNm> keyVars, final List<QNm> postVars,
      final List<Integer> keySides, final List<String> keyFields, final List<Integer> aggAt, final Set<QNm> seenKeys) {
    final Set<String> names = new HashSet<>(entries);
    for (int i = 0; i < entries; i++) {
      final AST entry = record.getChild(i);
      if (entry.getType() != XQ.KeyValueField || entry.getChildCount() != 2 || entry.getChild(0).getType() != XQ.Str) {
        return "return: entry is not a named field";
      }
      final String name = entry.getChild(0).getValue() instanceof Str str
          ? str.stringValue()
          : String.valueOf(entry.getChild(0).getValue());
      if (!names.add(name)) {
        return "return: duplicate output field name";
      }
      final AST value = entry.getChild(1);
      if (value.getType() != XQ.VariableRef || !(value.getValue() instanceof QNm var)) {
        return "return: entry value is not a variable reference";
      }
      entryVars.add(var);
      if (keyVars.contains(var)) {
        if (!seenKeys.add(var)) {
          return "return: key emitted twice";
        }
        final SideLet let = lets.get(indexOf(lets, var));
        entryKinds[i] = keySides.size();
        keySides.add(let.side());
        keyFields.add(let.field());
      } else if (postVars.contains(var)) {
        entryKinds[i] = -(aggAt.size() + 1);
        aggAt.add(postVars.indexOf(var));
      } else {
        return "return: entry is neither a key nor a post-group aggregate";
      }
    }
    return null;
  }

  private static @Nullable String rowEntries(final AST record, final int count, final QNm[] vars, final int[] keySides,
      final String[] keyFields, final int[] entryKinds, final Set<String> names) {
    for (int i = 0; i < count; i++) {
      final AST entry = record.getChild(i);
      if (entry.getType() != XQ.KeyValueField || entry.getChildCount() != 2) {
        return "row join: unsupported output entry";
      }
      final String name = stringLiteral(entry.getChild(0));
      if (name == null || !names.add(name)) {
        return "row join: output names must be distinct literals";
      }
      keySides[i] = ComputedProgram.loopVarDerefField(entry.getChild(1), vars[0]) == null
          ? 1
          : 0;
      keyFields[i] = ComputedProgram.loopVarDerefField(entry.getChild(1), vars[keySides[i]]);
      if (keyFields[i] == null) {
        return "row join: output is not a direct field";
      }
      entryKinds[i] = i;
    }
    return null;
  }

  private static @Nullable String rowOrder(final AST current, final QNm[] vars, final int count,
      final String[] keyFields, final int[] keySides, final List<Integer> orderIndexes, final List<Boolean> orderAsc,
      final List<Boolean> emptyLeast) {
    for (int i = 0; i < current.getChildCount() - 1; i++) {
      final AST spec = current.getChild(i);
      if (spec.getType() != XQ.OrderBySpec || spec.getChildCount() < 1) {
        return "row join: unsupported ordering";
      }
      int at = -1;
      for (int field = 0; field < count; field++) {
        if (keyFields[field].equals(ComputedProgram.loopVarDerefField(spec.getChild(0), vars[keySides[field]]))) {
          at = field;
          break;
        }
      }
      if (at < 0) {
        return "row join: ordering field is not emitted";
      }
      boolean ascending = true;
      boolean least = true;
      for (int m = 1; m < spec.getChildCount(); m++) {
        final AST modifier = spec.getChild(m);
        if (modifier.getType() == XQ.OrderByKind) {
          ascending = modifier.getChild(0).getType() == XQ.ASCENDING;
        } else if (modifier.getType() == XQ.OrderByEmptyMode) {
          least = modifier.getChild(0).getType() == XQ.LEAST;
        } else {
          return "row join: unsupported ordering modifier";
        }
      }
      orderIndexes.add(at);
      orderAsc.add(ascending);
      emptyLeast.add(least);
    }
    return null;
  }

  private record RangeBound(AST comparison, String field, AST limit, boolean start, boolean strict, boolean general,
      boolean fieldLeft) {
  }

  private static void selections(AST node, final List<AST> comparisons) {
    while (node.getType() == XQ.Selection && node.getChildCount() == 2) {
      conjuncts(node.getChild(0), comparisons);
      node = node.getLastChild();
    }
  }

  private static void conjuncts(final AST node, final List<AST> comparisons) {
    if (node.getType() == XQ.AndExpr && node.getChildCount() == 2) {
      conjuncts(node.getChild(0), comparisons);
      conjuncts(node.getChild(1), comparisons);
    } else {
      comparisons.add(node);
    }
  }

  private static @Nullable RangeBound rangeBound(final AST node, final QNm var) {
    if (node.getType() != XQ.ComparisonExpr || node.getChildCount() != 3) {
      return null;
    }
    final int op = node.getChild(0).getType();
    final int code = comparisonOpcode(op);
    if (code < 2) {
      return null;
    }
    for (int i = 1; i <= 2; i++) {
      final AST operand = node.getChild(i);
      final AST limit = node.getChild(3 - i);
      if (!dateTimeCast(operand)
          || !(limit.getType() == XQ.VariableRef || (dateTimeCast(limit) && limit.getChild(0).getType() == XQ.Str))
          || (limit.getType() == XQ.VariableRef && var.equals(limit.getValue()))) {
        continue;
      }
      final String field = ComputedProgram.loopVarDerefField(operand.getChild(0), var);
      if (field != null) {
        return new RangeBound(node, field, limit, (code <= 3) == (i == 1), code == 2 || code == 4,
            op == XQ.GeneralCompLT || op == XQ.GeneralCompLE || op == XQ.GeneralCompGT || op == XQ.GeneralCompGE,
            i == 1);
      }
    }
    return null;
  }

  /** Only the served annotations consume these predicates; the generic join remains intact. */
  private static @Nullable AST rangeSource(final AST source, final List<AST> comparisons, final QNm var,
      final QNm[] loopVars, final Set<AST> consumed) {
    final AST doc;
    AST normalized = source;
    while (normalized.getType() == XQ.ParenthesizedExpr && normalized.getChildCount() == 1) {
      normalized = normalized.getChild(0);
    }
    final boolean composed = !arrayMembers(source);
    if (composed) {
      final IndexRoutedSourceStage.Source routed = IndexRoutedSourceStage.source(source);
      if (routed == null) {
        return null;
      }
      if (routed.txTime() == null) {
        doc = normalized.getChild(0);
      } else {
        final AST time = routed.txTime();
        if (!(time.getType() == XQ.VariableRef || (dateTimeCast(time) && time.getChild(0).getType() == XQ.Str))) {
          return null;
        }
        doc = new AST(XQ.FunctionCall, new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open"));
        doc.addChild(new AST(XQ.Str, new Str(routed.database())));
        doc.addChild(new AST(XQ.Str, new Str(routed.resource())));
        doc.addChild(time.copyTree());
      }
    } else {
      doc = source.getChild(0);
    }
    if (doc.getType() != XQ.FunctionCall || !(doc.getValue() instanceof QNm fn)
        || !JSONFun.JSON_NSURI.equals(fn.getNamespaceURI())
        || !("doc".equals(fn.getLocalName()) || "open".equals(fn.getLocalName()))) {
      return null;
    }
    RangeBound start = null;
    RangeBound end = null;
    for (final AST comparison : comparisons) {
      final RangeBound bound = rangeBound(comparison, var);
      if (bound != null && !(bound.limit().getType() == XQ.VariableRef
          && (loopVars[0].equals(bound.limit().getValue()) || loopVars[1].equals(bound.limit().getValue())))) {
        if (bound.start() && start == null) {
          start = bound;
        } else if (!bound.start() && end == null) {
          end = bound;
        }
      }
    }
    if (start == null || end == null) {
      return null;
    }
    final AST call = new AST(XQ.FunctionCall, ScanValidTimeIndex.SCAN_VALID_TIME_INDEX);
    call.setProperty(ScanValidTimeIndex.DEFERRED_RANGE, true);
    call.addChild(doc.copyTree());
    call.addChild(start.limit().copyTree());
    call.addChild(new AST(XQ.Str, new Str(start.field())));
    call.addChild(new AST(XQ.Str, new Str(end.field())));
    final int mode = (start.strict()
        ? 1
        : 0)
        | (end.strict()
            ? 2
            : 0)
        | (comparisons.indexOf(end.comparison()) < comparisons.indexOf(start.comparison())
            ? 4
            : 0)
        | (start.general()
            ? 8
            : 0)
        | (end.general()
            ? 16
            : 0)
        | (start.fieldLeft()
            ? 0
            : 32)
        | (end.fieldLeft()
            ? 64
            : 0);
    call.addChild(new AST(XQ.Int, new Int32(mode)));
    call.addChild(end.limit().copyTree());
    if (composed) {
      call.addChild(source.copyTree());
    }
    consumed.add(start.comparison());
    consumed.add(end.comparison());
    return call;
  }

  private static boolean dateTimeCast(final AST node) {
    return node.getType() == XQ.FunctionCall && node.getChildCount() == 1 && node.getValue() instanceof QNm fn
        && "http://www.w3.org/2001/XMLSchema".equals(fn.getNamespaceURI()) && "dateTime".equals(fn.getLocalName());
  }

  private static int comparisonOpcode(final int op) {
    return switch (op) {
      case XQ.ValueCompEQ, XQ.GeneralCompEQ -> 0;
      case XQ.ValueCompNE, XQ.GeneralCompNE -> 1;
      case XQ.ValueCompLT, XQ.GeneralCompLT -> 2;
      case XQ.ValueCompLE, XQ.GeneralCompLE -> 3;
      case XQ.ValueCompGT, XQ.GeneralCompGT -> 4;
      case XQ.ValueCompGE, XQ.GeneralCompGE -> 5;
      default -> -1;
    };
  }

  private static @Nullable Program pairedProgram(final AST expression, final QNm[] vars, final List<String> fields) {
    final AST copy = expression.copyTree();
    final QNm paired = new QNm("paired");
    if (!pairOperands(copy, vars, paired)) {
      return null;
    }
    return ComputedProgram.build(copy, paired, fields);
  }

  private static boolean pairOperands(final AST node, final QNm[] vars, final QNm paired) {
    if (node.getType() == XQ.DerefExpr) {
      for (int side = 0; side < 2; side++) {
        final String field = ComputedProgram.loopVarDerefField(node, vars[side]);
        if (field != null) {
          node.replaceChild(0, new AST(XQ.VariableRef, paired));
          node.replaceChild(1, new AST(XQ.QNm, new QNm(side + "." + field)));
          return true;
        }
      }
      return false;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (!pairOperands(node.getChild(i), vars, paired)) {
        return false;
      }
    }
    return true;
  }

  private static boolean equalityComparison(final int op) {
    return !(op != XQ.ValueCompEQ && op != XQ.GeneralCompEQ && op != XQ.ValueCompNE && op != XQ.GeneralCompNE);
  }

  private static @Nullable String documentRevision(final AST call, final int side, final int[] revisions) {
    if (call.getChildCount() == 3) {
      final Object revision = call.getChild(2).getType() == XQ.Int
          ? call.getChild(2).getValue()
          : null;
      if (!(revision instanceof IntNumeric number) || number.longValue() < 0
          || number.longValue() > Integer.MAX_VALUE) {
        return "join: dynamic revision";
      }
      revisions[side] = (int) number.longValue();
    } else {
      revisions[side] = -1;
    }
    return null;
  }

  private static @Nullable AST residualSelections(AST tail, final QNm[] vars, final List<Integer> residualSides,
      final List<String> residualFields, final List<Integer> residualOps, final List<Integer> residualCode,
      final Set<AST> ranges) {
    while (tail.getType() == XQ.Selection && tail.getChildCount() == 2) {
      final int previous = residualCode.size();
      if (!residual(tail.getChild(0), vars, residualSides, residualFields, residualOps, residualCode, ranges)) {
        return null;
      }
      if (previous > 0 && residualCode.size() > previous) {
        residualCode.add(-1);
      }
      tail = tail.getLastChild();
    }
    return tail;
  }

  private static boolean booleanResidual(final AST node) {
    return (node.getType() == XQ.AndExpr || node.getType() == XQ.OrExpr) && node.getChildCount() == 2;
  }

  private static boolean arrayMembers(final AST call) {
    return call.getType() == XQ.ArrayAccess && call.getChildCount() == 2
        && call.getChild(1).getType() == XQ.SequenceExpr && call.getChild(1).getChildCount() == 0;
  }

  private static boolean unaryFunction(final AST call) {
    return call.getType() == XQ.FunctionCall && call.getChildCount() == 1;
  }

  private static boolean orderedRowRecord(final AST current) {
    return !(current.getType() != XQ.OrderBy || current.getLastChild().getType() != XQ.End
        || current.getLastChild().getChildCount() != 1
        || current.getLastChild().getChild(0).getType() != XQ.ObjectConstructor);
  }

}
