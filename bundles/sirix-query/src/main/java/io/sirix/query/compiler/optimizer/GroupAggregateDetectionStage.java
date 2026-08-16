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
  /**
   * {@code long[]}: per GROUP KEY, the constant offset of a shifted key ({@code group by $w} with
   * {@code $w := $r.f + k}); 0 for an untransformed key. The kernel groups on the TRANSFORMED
   * value — grouping raw and shifting at emission would be wrong only for non-injective
   * transforms, but the discipline is uniform.
   */
  public static final String GROUP_AGG_KEY_OFFSETS = "SIRIX_GROUP_AGG_KEY_OFFSETS";
  /**
   * {@code int[]} of {@code 2 * keyCount}: per group key, the substring transform — slot
   * {@code 2k} is {@code 0} for none, {@code +start} for {@code xs:integer(substring(f, s, l))}
   * (grouping on the CAST integer), or {@code -start} for a bare {@code substring(f, s, l)}
   * (grouping on the STRING, served via an order-preserving digit pack over validated ISO-minute
   * windows); slot {@code 2k+1} is the length. The kernel evaluates transforms once per
   * dictionary entry per leaf. RAISE/unpackable cases DECLINE: for the int cast the interpreter
   * raises on a bad slice; for the string variant a window failing the ISO shape (including the
   * {@code ""} a MISSING field produces — a REAL group key, never the null-key group) would
   * corrupt the packed order, so the generic pipeline serves it instead.
   */
  public static final String GROUP_AGG_KEY_SUBSTR = "SIRIX_GROUP_AGG_KEY_SUBSTR";
  /**
   * {@code int[]} / {@code String[]} / {@code String[]}: canonical record positions of KEY
   * entries emitted through {@code concat($key, "lit")} / {@code concat("lit", $key)}, with the
   * literal prefix/suffix to decorate the served key value with — applied by the serving
   * expression over the K emitted records, before constant-entry splicing.
   */
  public static final String GROUP_AGG_KEY_DECOR_POS = "SIRIX_GROUP_AGG_KEY_DECOR_POS";
  public static final String GROUP_AGG_KEY_DECOR_PREFIX = "SIRIX_GROUP_AGG_KEY_DECOR_PREFIX";
  public static final String GROUP_AGG_KEY_DECOR_SUFFIX = "SIRIX_GROUP_AGG_KEY_DECOR_SUFFIX";
  /** {@code int[]} / {@code String[]} / {@code long[]}: record positions, field names and integer
   * literals of CONSTANT key entries ({@code group by $one, $k} with {@code $one := 1}) — they
   * partition nothing, and the serving expression splices them back into each record. */
  public static final String GROUP_AGG_CONST_ENTRY_POS = "SIRIX_GROUP_AGG_CONST_ENTRY_POS";
  public static final String GROUP_AGG_CONST_ENTRY_NAMES = "SIRIX_GROUP_AGG_CONST_ENTRY_NAMES";
  public static final String GROUP_AGG_CONST_ENTRY_VALUES = "SIRIX_GROUP_AGG_CONST_ENTRY_VALUES";
  /**
   * CONDITIONAL key transform (the Q39 {@code CASE WHEN} port):
   * {@code let $k := if ($r.c1 = L1 [and $r.c2 = L2]) then $r.f else "lit"}. Three parallel
   * properties: {@code String[2*keyCount]} condition fields (slot {@code 2k+1} {@code null} for a
   * single-conjunct condition; both {@code null} = key not conditional), {@code long[2*keyCount]}
   * the integer literals compared against, and {@code String[keyCount]} the else-branch string
   * literal ({@code null} = key not conditional — the authoritative marker). The kernel evaluates
   * the condition per row from the numeric condition columns (missing ⇒ false, the general
   * comparison's existential); the then-branch reads the dict component (missing ⇒ the
   * empty-sequence key, exactly the untransformed deref's behavior); the else branch hashes the
   * literal's bytes in the SAME domain as dict entries, so a stored value equal to the literal
   * lands in the same group the interpreter puts it in.
   */
  public static final String GROUP_AGG_KEY_COND_FIELDS = "SIRIX_GROUP_AGG_KEY_COND_FIELDS";
  public static final String GROUP_AGG_KEY_COND_LITS = "SIRIX_GROUP_AGG_KEY_COND_LITS";
  public static final String GROUP_AGG_KEY_COND_ELSE = "SIRIX_GROUP_AGG_KEY_COND_ELSE";
  /**
   * HAVING (SQL) / post-group {@code where}: {@code long[]{op, literal}} filtering groups by
   * their COUNT — {@code where $c > 100000} with {@code $c := count($r)}. Op encoding:
   * 0 {@code >}, 1 {@code >=}, 2 {@code <}, 3 {@code <=}, 4 {@code =}, 5 {@code !=}. Applied
   * BEFORE top-K selection (a filtered group must not occupy a window slot). v1 recognizes ONE
   * such selection over a post-group count let; anything else declines the pipeline.
   */
  public static final String GROUP_AGG_HAVING = "SIRIX_GROUP_AGG_HAVING";

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
    // Per pre-group let: the constant added to the deref (0 for a plain `$r.field`).
    final List<Long> letOffsets = new ArrayList<>();
    // Per pre-group let: (start, length) of an xs:integer(substring(...)) binding, or null.
    final List<int[]> letSubstr = new ArrayList<>();
    // Per pre-group let: the conditional shape `if (cond) then $r.f else "lit"`, or null.
    final List<CondDeref> letCond = new ArrayList<>();
    final String[] subField = new String[1];
    // Pre-group lets bound to a LITERAL: constant group keys (`let $g := 1 ... group by $g`).
    final List<QNm> constLetVars = new ArrayList<>();
    final List<Long> constLetVals = new ArrayList<>();
    final List<QNm> groupSpecVars = new ArrayList<>();
    long[] havingOpLit = null;
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
          // A selection AFTER group-by is HAVING-shaped. ONE `$countVar OP intLiteral`
          // over a post-group count let serves as a group filter (applied before top-K);
          // anything else declines — serving it as a ROW filter would change counts.
          if (hasGroupBy) {
            if (havingOpLit != null || !orderVars.isEmpty()) {
              return; // one HAVING, and only between the aggregate lets and the order-by
            }
            havingOpLit = havingCountFilter(current.getChild(0), postGroupVars, postGroupFuncs, postGroupFields);
            if (havingOpLit == null) {
              return;
            }
            continue;
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
            final Agg agg = aggregateCall(current.getChild(1), loopVar, letVars, letFields, letOffsets, letSubstr,
                letCond);
            if (agg == null) {
              return;
            }
            postGroupVars.add(letVar);
            postGroupFuncs.add(agg.func());
            postGroupFields.add(agg.field());
            postGroupOffsets.add(agg.offset());
          } else {
            AST bound = current.getChild(1);
            // `let $x := (expr)` arrives ParenthesizedExpr-wrapped (the Q40 lesson: the parser
            // keeps parens until later stages). A SINGLE-child wrap is transparent; a
            // multi-child one is a sequence literal and stays — no recognizer claims it.
            while (bound.getType() == XQ.ParenthesizedExpr && bound.getChildCount() == 1) {
              bound = bound.getChild(0);
            }
            final String field = loopVarDerefField(bound, loopVar);
            if (field != null) {
              letVars.add(letVar);
              letFields.add(field);
              letOffsets.add(0L);
              letSubstr.add(null);
              letCond.add(null);
            } else if (bound.getType() == XQ.Int) {
              // A literal binding: a CONSTANT group key candidate (`let $g := 1`). Only a
              // group-by spec (and the record entry echoing it) may consume it.
              final long lit = intValue(bound);
              if (lit == Long.MIN_VALUE) {
                return;
              }
              constLetVars.add(letVar);
              constLetVals.add(lit);
            } else {
              final ShiftedDeref shifted = shiftedDeref(bound, loopVar);
              if (shifted != null) {
                letVars.add(letVar);
                letFields.add(shifted.field());
                letOffsets.add(shifted.offset());
                letSubstr.add(null);
                letCond.add(null);
                continue;
              }
              // string-length($r.f): encoded as the "len:" field prefix — every later layer
              // (agg roster, accumulator blocks, ordering) treats "len:f" as its own operand.
              // fn:string-length(()) is 0, NOT empty: a row missing f contributes 0 to the
              // aggregate, which the kernel's strlen mode reproduces (fold 0, never skip).
              final String strlenField = strlenCall(bound, loopVar);
              if (strlenField != null) {
                letVars.add(letVar);
                letFields.add("len:" + strlenField);
                letOffsets.add(0L);
                letSubstr.add(null);
                letCond.add(null);
                continue;
              }
              final CondDeref cond = conditionalDeref(bound, loopVar);
              if (cond != null) {
                letVars.add(letVar);
                letFields.add(cond.thenField());
                letOffsets.add(0L);
                letSubstr.add(null);
                letCond.add(cond);
                continue;
              }
              int[] sub = integerOfSubstring(bound, loopVar, subField);
              int subKind = 0;
              if (sub == null) {
                sub = substringCall(bound, loopVar, subField);
                subKind = 1; // bare substring: a STRING-valued key transform
              }
              if (sub == null) {
                return; // a let we can't model — the served scan would not see it
              }
              letVars.add(letVar);
              letFields.add(subField[0]);
              letOffsets.add(0L);
              letSubstr.add(new int[] {sub[0], sub[1], subKind});
              letCond.add(null);
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
    if (!constMode && (keyCount < 1 || keyCount > MAX_GROUP_KEYS)) {
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
    // Pure constant grouping (Q29) does NOT emit its keys — the record is aggregates only.
    // MIXED grouping (Q34) must emit every key, constant entries included.
    final int keyTotal = constMode
        ? 0
        : keyCount + constKeySpecs.size();
    final AST returnExpr = current.getChild(0);
    if (returnExpr == null || returnExpr.getType() != XQ.ObjectConstructor
        || returnExpr.getChildCount() < keyTotal + 1) {
      return;
    }
    // The first keyCount entries must be the group keys — one VariableRef per group var,
    // each group var exactly once, in any order. Record-entry order defines the served
    // key emission order (field order is answer shape).
    final String[] keyNames = new String[keyCount];
    final String[] groupFields = new String[keyCount];
    final long[] keyOffsets = new long[keyCount];
    final int[] keySubstr = new int[2 * keyCount];
    final String[] keyCondFields = new String[2 * keyCount];
    final long[] keyCondLits = new long[2 * keyCount];
    final String[] keyCondElse = new String[keyCount];
    boolean anyCondKey = false;
    final QNm[] realKeyVars = new QNm[keyCount];
    final List<Integer> decorPos = new ArrayList<>();
    final List<String> decorPrefixes = new ArrayList<>();
    final List<String> decorSuffixes = new ArrayList<>();
    final List<Integer> constEntryPos = new ArrayList<>();
    final List<String> constEntryNames = new ArrayList<>();
    final List<Long> constEntryVals = new ArrayList<>();
    boolean anyKeyTransform = false;
    final Set<String> seenNames = new HashSet<>();
    final Set<QNm> seenGroupVars = new HashSet<>();
    final Set<QNm> seenConstVars = new HashSet<>();
    int realIdx = 0;
    for (int i = 0; i < keyTotal; i++) {
      final AST keyEntry = returnExpr.getChild(i);
      final String keyName = kvName(keyEntry);
      if (keyName == null || !seenNames.add(keyName)) {
        return;
      }
      AST keyValue = keyEntry.getChild(1);
      String decorPrefix = null;
      String decorSuffix = null;
      if (keyValue.getType() == XQ.FunctionCall && keyValue.getChildCount() == 2
          && keyValue.getValue() instanceof QNm cfn && "concat".equals(cfn.getLocalName())) {
        final String cns = cfn.getNamespaceURI();
        if (cns != null && !cns.isEmpty() && !Namespaces.FN_NSURI.equals(cns)
            && !Namespaces.DEFAULT_FN_NSURI.equals(cns)) {
          return;
        }
        final AST a0 = keyValue.getChild(0);
        final AST a1 = keyValue.getChild(1);
        if (a0.getType() == XQ.VariableRef && a1.getType() == XQ.Str && a1.getValue() instanceof Str sfx) {
          decorSuffix = sfx.stringValue();
          keyValue = a0;
        } else if (a1.getType() == XQ.VariableRef && a0.getType() == XQ.Str && a0.getValue() instanceof Str pfx) {
          decorPrefix = pfx.stringValue();
          keyValue = a1;
        } else {
          return;
        }
      }
      if (keyValue.getType() != XQ.VariableRef || !(keyValue.getValue() instanceof QNm keyVar)) {
        return;
      }
      if (constKeySpecs.contains(keyVar)) {
        // A CONSTANT key entry: it partitions nothing — the expression splices the literal
        // back into every served record at this position.
        if (decorPrefix != null || decorSuffix != null || !seenConstVars.add(keyVar)) {
          return;
        }
        constEntryPos.add(i);
        constEntryNames.add(keyName);
        constEntryVals.add(constLetVals.get(constLetVars.indexOf(keyVar)));
        continue;
      }
      if (!groupSpecVars.contains(keyVar) || !seenGroupVars.add(keyVar)) {
        return;
      }
      final int letIdx = letVars.indexOf(keyVar);
      if (letIdx < 0) {
        return;
      }
      realKeyVars[realIdx] = keyVar;
      keyNames[realIdx] = keyName;
      groupFields[realIdx] = letFields.get(letIdx);
      keyOffsets[realIdx] = letOffsets.get(letIdx);
      final int[] sub = letSubstr.get(letIdx);
      keySubstr[2 * realIdx] = sub == null
          ? 0
          : sub[2] == 1
              ? -sub[0]
              : sub[0];
      keySubstr[2 * realIdx + 1] = sub == null
          ? 0
          : sub[1];
      final CondDeref keyCond = letCond.get(letIdx);
      if (keyCond != null) {
        keyCondFields[2 * realIdx] = keyCond.condField1();
        keyCondLits[2 * realIdx] = keyCond.condLit1();
        keyCondFields[2 * realIdx + 1] = keyCond.condField2();
        keyCondLits[2 * realIdx + 1] = keyCond.condLit2();
        keyCondElse[realIdx] = keyCond.elseLit();
        anyCondKey = true;
      }
      anyKeyTransform |= keyOffsets[realIdx] != 0L || sub != null || keyCond != null;
      if (decorPrefix != null || decorSuffix != null) {
        decorPos.add(realIdx);
        decorPrefixes.add(decorPrefix == null
            ? ""
            : decorPrefix);
        decorSuffixes.add(decorSuffix == null
            ? ""
            : decorSuffix);
      }
      realIdx++;
    }
    if (realIdx != keyCount || (!constMode && constEntryPos.size() != constKeySpecs.size())) {
      // Some spec var was never emitted — the record does not echo the grouping. Pure const
      // mode is exempt: its keys are legitimately unemitted (the record is aggregates only).
      return;
    }
    final int aggCount = returnExpr.getChildCount() - keyTotal;
    final String[] funcs = new String[aggCount];
    final String[] fields = new String[aggCount];
    final String[] outNames = new String[aggCount];
    final long[] offsets = new long[aggCount];
    // Which emitted entry carries each post-group let var, so an order-by on that var resolves
    // to a field of the answer record instead of a value only the interpreter can recompute.
    final List<QNm> emittedPostGroupVars = new ArrayList<>();
    final List<Integer> emittedPostGroupAt = new ArrayList<>();
    for (int i = 0; i < aggCount; i++) {
      final AST entry = returnExpr.getChild(keyTotal + i);
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
        agg = aggregateCall(value, loopVar, letVars, letFields, letOffsets, letSubstr, letCond);
        if (agg == null) {
          return;
        }
      }
      if (agg.offset() != 0L && !constMode) {
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
      int keyAt = -1;
      for (int r = 0; r < keyCount; r++) {
        if (orderVar.equals(realKeyVars[r])) {
          keyAt = r;
          break;
        }
      }
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
    if (constMode && havingOpLit != null) {
      return; // one group + HAVING: honoring it means maybe-empty output — not this shape (v1)
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
    if (havingOpLit != null) {
      pipeExpr.setProperty(GROUP_AGG_HAVING, havingOpLit);
    }
    pipeExpr.setProperty(GROUP_AGG_GROUP_FIELDS, groupFields);
    pipeExpr.setProperty(GROUP_AGG_KEY_NAMES, keyNames);
    if (anyKeyTransform) {
      pipeExpr.setProperty(GROUP_AGG_KEY_OFFSETS, keyOffsets);
      pipeExpr.setProperty(GROUP_AGG_KEY_SUBSTR, keySubstr);
    }
    if (anyCondKey) {
      pipeExpr.setProperty(GROUP_AGG_KEY_COND_FIELDS, keyCondFields);
      pipeExpr.setProperty(GROUP_AGG_KEY_COND_LITS, keyCondLits);
      pipeExpr.setProperty(GROUP_AGG_KEY_COND_ELSE, keyCondElse);
    }
    if (!decorPos.isEmpty()) {
      final int[] dp = new int[decorPos.size()];
      for (int i = 0; i < dp.length; i++) {
        dp[i] = decorPos.get(i);
      }
      pipeExpr.setProperty(GROUP_AGG_KEY_DECOR_POS, dp);
      pipeExpr.setProperty(GROUP_AGG_KEY_DECOR_PREFIX, decorPrefixes.toArray(new String[0]));
      pipeExpr.setProperty(GROUP_AGG_KEY_DECOR_SUFFIX, decorSuffixes.toArray(new String[0]));
    }
    if (!constEntryPos.isEmpty()) {
      final int[] posArr = new int[constEntryPos.size()];
      final long[] valArr = new long[constEntryPos.size()];
      for (int i = 0; i < posArr.length; i++) {
        posArr[i] = constEntryPos.get(i);
        valArr[i] = constEntryVals.get(i);
      }
      pipeExpr.setProperty(GROUP_AGG_CONST_ENTRY_POS, posArr);
      pipeExpr.setProperty(GROUP_AGG_CONST_ENTRY_NAMES, constEntryNames.toArray(new String[0]));
      pipeExpr.setProperty(GROUP_AGG_CONST_ENTRY_VALUES, valArr);
    }
    pipeExpr.setProperty(GROUP_AGG_FUNCS, funcs);
    pipeExpr.setProperty(GROUP_AGG_FIELDS, fields);
    pipeExpr.setProperty(GROUP_AGG_OUT_NAMES, outNames);
    if (orderCount > 0) {
      pipeExpr.setProperty(GROUP_AGG_ORDER_INDEXES, orderIndexes);
      pipeExpr.setProperty(GROUP_AGG_ORDER_ASC, orderAscending);
      pipeExpr.setProperty(GROUP_AGG_ORDER_EMPTY_LEAST, orderEmptyLeastFlags);
    }
  }

  /** A servable aggregate: {@code field} is {@code null} for {@code count}; {@code offset} is the
   * constant a shifted operand let added ({@code 0} for a plain deref). */
  private record Agg(String func, String field, long offset) {
  }

  /** A pre-group let's shifted operand {@code $loop.field + k} / {@code k + $loop.field} /
   * {@code $loop.field - k}. */
  private record ShiftedDeref(String field, long offset) {
  }

  /** A pre-group let's conditional key {@code if ($loop.c1 = L1 [and $loop.c2 = L2]) then
   * $loop.f else "lit"} — the SQL {@code CASE WHEN} port. {@code condField2} is {@code null}
   * for a single-conjunct condition. */
  private record CondDeref(String condField1, long condLit1, String condField2, long condLit2, String thenField,
      String elseLit) {
  }

  /**
   * Recognize the conditional key shape. Strict on purpose: the condition is one or two
   * {@code $loop.field = <int literal>} GENERAL comparisons (missing ⇒ false, the kernel's
   * existential), the then-branch a direct deref, the else-branch a string literal. Anything
   * wider (value comparisons, nested conditions, non-literal branches) declines the let.
   */
  private static CondDeref conditionalDeref(final AST expr, final QNm loopVar) {
    if (expr == null || expr.getType() != XQ.IfExpr || expr.getChildCount() != 3) {
      return null;
    }
    final AST cond = expr.getChild(0);
    final String thenField = loopVarDerefField(expr.getChild(1), loopVar);
    final AST elseBranch = expr.getChild(2);
    if (thenField == null || elseBranch.getType() != XQ.Str || !(elseBranch.getValue() instanceof Str elseLit)) {
      return null;
    }
    final long[] lit = new long[1];
    if (cond.getType() == XQ.AndExpr && cond.getChildCount() == 2) {
      final String f1 = eqCondField(cond.getChild(0), loopVar, lit);
      final long l1 = lit[0];
      if (f1 == null) {
        return null;
      }
      final String f2 = eqCondField(cond.getChild(1), loopVar, lit);
      if (f2 == null) {
        return null;
      }
      return new CondDeref(f1, l1, f2, lit[0], thenField, elseLit.stringValue());
    }
    final String f1 = eqCondField(cond, loopVar, lit);
    return f1 == null
        ? null
        : new CondDeref(f1, lit[0], null, 0L, thenField, elseLit.stringValue());
  }

  /** {@code fn:string-length($loop.field)} (built-in namespace only) → the field name, else
   * {@code null}. */
  private static String strlenCall(final AST expr, final QNm loopVar) {
    if (expr == null || expr.getType() != XQ.FunctionCall || expr.getChildCount() != 1
        || !(expr.getValue() instanceof QNm fn) || !"string-length".equals(fn.getLocalName())) {
      return null;
    }
    final String ns = fn.getNamespaceURI();
    if (ns != null && !ns.isEmpty() && !Namespaces.FN_NSURI.equals(ns) && !Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
      return null;
    }
    return loopVarDerefField(expr.getChild(0), loopVar);
  }

  /**
   * {@code $countVar OP <int literal>} (either operand order, general comparisons only) where
   * {@code $countVar} is a post-group {@code count($loop)} let → {@code long[]{op, literal}}
   * under {@link #GROUP_AGG_HAVING}'s encoding; else {@code null}. Counts are the only v1
   * operand: every other aggregate's lane may be absent for a group (empty min over a missing
   * field), and a filter over an empty operand needs the interpreter's comparison semantics.
   */
  private static long[] havingCountFilter(final AST pred, final List<QNm> postGroupVars,
      final List<String> postGroupFuncs, final List<String> postGroupFields) {
    if (pred == null || pred.getType() != XQ.ComparisonExpr || pred.getChildCount() != 3) {
      return null;
    }
    final int cmp = pred.getChild(0).getType();
    for (int side = 0; side < 2; side++) {
      final AST varSide = pred.getChild(1 + side);
      final long lit = intValue(pred.getChild(2 - side));
      if (lit == Long.MIN_VALUE || varSide.getType() != XQ.VariableRef
          || !(varSide.getValue() instanceof QNm var)) {
        continue;
      }
      final int at = postGroupVars.indexOf(var);
      if (at < 0 || !"count".equals(postGroupFuncs.get(at)) || postGroupFields.get(at) != null) {
        return null;
      }
      long op;
      if (cmp == XQ.GeneralCompGT) {
        op = 0;
      } else if (cmp == XQ.GeneralCompGE) {
        op = 1;
      } else if (cmp == XQ.GeneralCompLT) {
        op = 2;
      } else if (cmp == XQ.GeneralCompLE) {
        op = 3;
      } else if (cmp == XQ.GeneralCompEQ) {
        op = 4;
      } else if (cmp == XQ.GeneralCompNE) {
        op = 5;
      } else {
        return null;
      }
      if (side == 1) {
        // literal OP $c — mirror the relation.
        op = switch ((int) op) {
          case 0 -> 2L;
          case 1 -> 3L;
          case 2 -> 0L;
          case 3 -> 1L;
          default -> op;
        };
      }
      return new long[] {op, lit};
    }
    return null;
  }

  /** {@code $loop.field = <int literal>} (either operand order, general {@code =} only) →
   * the field name, with the literal in {@code litOut[0]}; else {@code null}. */
  private static String eqCondField(final AST cmp, final QNm loopVar, final long[] litOut) {
    if (cmp == null || cmp.getType() != XQ.ComparisonExpr || cmp.getChildCount() != 3
        || cmp.getChild(0).getType() != XQ.GeneralCompEQ) {
      return null;
    }
    for (int side = 0; side < 2; side++) {
      final String field = loopVarDerefField(cmp.getChild(1 + side), loopVar);
      final long lit = intValue(cmp.getChild(2 - side));
      if (field != null && lit != Long.MIN_VALUE) {
        litOut[0] = lit;
        return field;
      }
    }
    return null;
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
      final List<String> letFields, final List<Long> letOffsets, final List<int[]> letSubstr,
      final List<CondDeref> letCond) {
    if (call == null || call.getType() != XQ.FunctionCall || call.getChildCount() != 1
        || !(call.getValue() instanceof QNm fn)) {
      return null;
    }
    // xs:double(<aggregate>) — SQL AVG is a double, so every ported benchmark wraps avg this way,
    // and the wrap must not decline the whole pipeline. The token is PREFIXED ("dbl:avg") so the
    // cast flows through every annotation layer unchanged; emission applies Brackit's own cast to
    // the exact value, digit-for-digit what the interpreter's constructor function computes.
    if (Namespaces.XS_NSURI.equals(fn.getNamespaceURI()) && "double".equals(fn.getLocalName())) {
      final Agg inner = aggregateCall(call.getChild(0), loopVar, letVars, letFields, letOffsets, letSubstr, letCond);
      return inner == null || inner.func().startsWith("dbl:")
          ? null
          : new Agg("dbl:" + inner.func(), inner.field(), inner.offset());
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
      if (arg.getType() == XQ.VariableRef && loopVar.equals(arg.getValue())) {
        return new Agg(func, null, 0L);
      }
      // count(distinct-values($r.f)) — the grouped COUNT(DISTINCT). Emitted as its own token,
      // deliberately NOT in VALUE_FUNCS: a user function literally named `count-distinct` can
      // never produce it, and the single-argument value funcs never see a nested call.
      if (arg.getType() == XQ.FunctionCall && arg.getChildCount() == 1 && arg.getValue() instanceof QNm inner
          && "distinct-values".equals(inner.getLocalName())) {
        final String ins = inner.getNamespaceURI();
        if (ins == null || ins.isEmpty() || Namespaces.FN_NSURI.equals(ins) || Namespaces.DEFAULT_FN_NSURI.equals(ins)) {
          final AST dArg = arg.getChild(0);
          final String direct = loopVarDerefField(dArg, loopVar);
          if (direct != null) {
            return new Agg("count-distinct", direct, 0L);
          }
          if (dArg.getType() == XQ.VariableRef && dArg.getValue() instanceof QNm dv) {
            final int li = letVars.indexOf(dv);
            if (li >= 0 && letOffsets.get(li) == 0L && letSubstr.get(li) == null && letCond.get(li) == null) {
              // A SHIFTED let stays declined: distinct-count is shift-invariant in principle,
              // but proving that here buys nothing ClickBench-shaped.
              return new Agg("count-distinct", letFields.get(li), 0L);
            }
          }
        }
      }
      return null;
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
      if (letIdx >= 0 && letSubstr.get(letIdx) == null && letCond.get(letIdx) == null) {
        // A substring- or conditionally-transformed let as an operand would fold the RAW column.
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

  /**
   * {@code xs:integer(substring($loopVar.f, startLit, lenLit))} — the DATE_TRUNC/EXTRACT idiom
   * over ISO-8601 strings. Positive int literals only; the field lands in {@code fieldOut[0]}.
   */
  private static int[] integerOfSubstring(final AST expr, final QNm loopVar, final String[] fieldOut) {
    if (expr == null || expr.getType() != XQ.FunctionCall || expr.getChildCount() != 1
        || !(expr.getValue() instanceof QNm outer) || !"integer".equals(outer.getLocalName())
        || !Namespaces.XS_NSURI.equals(outer.getNamespaceURI())) {
      return null;
    }
    return substringCall(expr.getChild(0), loopVar, fieldOut);
  }

  /** The bare {@code substring($loopVar.f, startLit, lenLit)} — the DATE_TRUNC idiom's STRING
   * form. Positive int literals only; the field lands in {@code fieldOut[0]}. */
  private static int[] substringCall(final AST sub, final QNm loopVar, final String[] fieldOut) {
    if (sub == null || sub.getType() != XQ.FunctionCall || sub.getChildCount() != 3
        || !(sub.getValue() instanceof QNm fn) || !"substring".equals(fn.getLocalName())) {
      return null;
    }
    final String ns = fn.getNamespaceURI();
    if (ns != null && !ns.isEmpty() && !Namespaces.FN_NSURI.equals(ns) && !Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
      return null;
    }
    final String field = loopVarDerefField(sub.getChild(0), loopVar);
    if (field == null) {
      return null;
    }
    final long start = sub.getChild(1).getType() == XQ.Int
        ? intValue(sub.getChild(1))
        : Long.MIN_VALUE;
    final long len = sub.getChild(2).getType() == XQ.Int
        ? intValue(sub.getChild(2))
        : Long.MIN_VALUE;
    if (start < 1 || len < 0 || start > 1 << 20 || len > 1 << 20) {
      return null;
    }
    fieldOut[0] = field;
    return new int[] {(int) start, (int) len};
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
