package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.function.json.JSONFun;
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
 * (record field order is part of the serialized answer); aggregate arguments must be a deref chain
 * rooted at the loop variable ({@code $r.field} or {@code $r.a.b.field} — see
 * {@link #loopVarDerefField}) or a pre-group let bound to one; every {@code order by} spec must be
 * a bare variable that the return record EMITS (anything else would have to be recomputed outside
 * the scan) and must follow the group-by — a pre-group {@code order by} reorders rows and so
 * changes which tuple is first in each group, which is the emission order the served path
 * reproduces.
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
   * {@code sum(f+k) = sum(f) + k·presentCount(f)}, {@code min/max/avg(f+k) = min/max/avg(f) + k} (the
   * let's arithmetic over a MISSING f is the empty sequence, so exactly the present rows contribute —
   * the same population the plain aggregate folds).
   */
  public static final String GROUP_AGG_OFFSETS = "SIRIX_GROUP_AGG_OFFSETS";
  /**
   * {@code long[]}: per GROUP KEY, the constant offset of a shifted key ({@code group by $w} with
   * {@code $w := $r.f + k}); 0 for an untransformed key. The kernel groups on the TRANSFORMED value —
   * grouping raw and shifting at emission would be wrong only for non-injective transforms, but the
   * discipline is uniform.
   */
  public static final String GROUP_AGG_KEY_OFFSETS = "SIRIX_GROUP_AGG_KEY_OFFSETS";
  /**
   * {@code int[]} of {@code 2 * keyCount}: per group key, the substring transform — slot {@code 2k}
   * is {@code 0} for none, {@code +start} for {@code xs:integer(substring(f, s, l))} (grouping on the
   * CAST integer), or {@code -start} for a bare {@code substring(f, s, l)} (grouping on the STRING,
   * served via an order-preserving digit pack over validated ISO-minute windows); slot {@code 2k+1}
   * is the length. The kernel evaluates transforms once per dictionary entry per leaf.
   * RAISE/unpackable cases DECLINE: for the int cast the interpreter raises on a bad slice; for the
   * string variant a window failing the ISO shape (including the {@code ""} a MISSING field produces
   * — a REAL group key, never the null-key group) would corrupt the packed order, so the generic
   * pipeline serves it instead.
   */
  public static final String GROUP_AGG_KEY_SUBSTR = "SIRIX_GROUP_AGG_KEY_SUBSTR";
  /**
   * {@code int[]} / {@code String[]} / {@code String[]}: canonical record positions of KEY entries
   * emitted through {@code concat($key, "lit")} / {@code concat("lit", $key)}, with the literal
   * prefix/suffix to decorate the served key value with — applied by the serving expression over the
   * K emitted records, before constant-entry splicing.
   */
  public static final String GROUP_AGG_KEY_DECOR_POS = "SIRIX_GROUP_AGG_KEY_DECOR_POS";
  public static final String GROUP_AGG_KEY_DECOR_PREFIX = "SIRIX_GROUP_AGG_KEY_DECOR_PREFIX";
  public static final String GROUP_AGG_KEY_DECOR_SUFFIX = "SIRIX_GROUP_AGG_KEY_DECOR_SUFFIX";
  /**
   * {@code int[]} / {@code String[]} / {@code long[]}: record positions, field names and integer
   * literals of CONSTANT key entries ({@code group by $one, $k} with {@code $one := 1}) — they
   * partition nothing, and the serving expression splices them back into each record.
   */
  public static final String GROUP_AGG_CONST_ENTRY_POS = "SIRIX_GROUP_AGG_CONST_ENTRY_POS";
  public static final String GROUP_AGG_CONST_ENTRY_NAMES = "SIRIX_GROUP_AGG_CONST_ENTRY_NAMES";
  public static final String GROUP_AGG_CONST_ENTRY_VALUES = "SIRIX_GROUP_AGG_CONST_ENTRY_VALUES";
  /**
   * CONDITIONAL key transform (the Q39 {@code CASE WHEN} port):
   * {@code let $k := if ($r.c1 = L1 [and $r.c2 = L2]) then $r.f else "lit"}. Three parallel
   * properties: {@code String[2*keyCount]} condition fields (slot {@code 2k+1} {@code null} for a
   * single-conjunct condition; both {@code null} = key not conditional), {@code long[2*keyCount]} the
   * integer literals compared against, and {@code String[keyCount]} the else-branch string literal
   * ({@code null} = key not conditional — the authoritative marker). The kernel evaluates the
   * condition per row from the numeric condition columns (missing ⇒ false, the general comparison's
   * existential); the then-branch reads the dict component (missing ⇒ the empty-sequence key, exactly
   * the untransformed deref's behavior); the else branch hashes the literal's bytes in the SAME
   * domain as dict entries, so a stored value equal to the literal lands in the same group the
   * interpreter puts it in.
   */
  public static final String GROUP_AGG_KEY_COND_FIELDS = "SIRIX_GROUP_AGG_KEY_COND_FIELDS";
  public static final String GROUP_AGG_KEY_COND_LITS = "SIRIX_GROUP_AGG_KEY_COND_LITS";
  public static final String GROUP_AGG_KEY_COND_ELSE = "SIRIX_GROUP_AGG_KEY_COND_ELSE";
  /**
   * REGEX key transform (the Q28 {@code REGEXP_REPLACE} port): {@code let $k :=
   * replace($r.f, 'pattern', 'replacement')} with LITERAL pattern/replacement. Two String[]
   * properties parallel to the keys ({@code null} = key not regex-transformed). Grouping happens on
   * the TRANSFORMED string (FNV of its bytes); winners re-apply the transform at emission. The
   * executor validates the pattern through brackit's own Regex once (an invalid pattern or a
   * zero-length-matching one raises/declines) and precompiles a java Pattern — brackit's REPLACE mode
   * with no flags appends the pattern verbatim and substitutes with Matcher semantics, so the
   * precompiled application is byte-identical.
   */
  public static final String GROUP_AGG_KEY_REGEX_PATTERN = "SIRIX_GROUP_AGG_KEY_REGEX_PATTERN";
  public static final String GROUP_AGG_KEY_REGEX_REPL = "SIRIX_GROUP_AGG_KEY_REGEX_REPL";
  /**
   * INTEGER DATE-PART key transform: {@code long[2 * keyCount]}, per group key the divisor of
   * {@code $r.f idiv D} in slot {@code 2k} and the modulus of {@code ... mod M} in slot
   * {@code 2k + 1}; {@code 0} means the operation is absent, and both slots {@code 0} means the key
   * is not divmod-transformed. Grouping happens on the TRANSFORMED value — the transform is
   * deliberately NOT required to be injective, which is the whole point of {@code mod 24} — so
   * winners re-apply it at emission rather than emitting the raw column.
   */
  public static final String GROUP_AGG_KEY_DIVMOD = "SIRIX_GROUP_AGG_KEY_DIVMOD";
  /**
   * {@code boolean[keyCount]}: the key is {@code fn:string(<chain>)}. Only the MISSING-row behavior
   * differs from the bare deref — {@code fn:string(())} is {@code ""}, so those rows group with (and
   * print as) the empty string instead of forming a keyless group. The kernel therefore hashes
   * {@code ""} in the SAME dictionary domain as stored values, which is what makes a stored empty
   * string merge into the interpreter's single group; the guard that keeps the rest of
   * {@code fn:string} out of scope is the column kind, checked STRING_DICT at serve time (over a
   * numeric column {@code fn:string} would emit {@code "5"} where the kernel emits {@code 5}).
   */
  public static final String GROUP_AGG_KEY_STRINGIFY = "SIRIX_GROUP_AGG_KEY_STRINGIFY";
  /**
   * HAVING (SQL) / post-group {@code where}: {@code long[]{op, literal}} filtering groups by their
   * COUNT — {@code where $c > 100000} with {@code $c := count($r)}. Op encoding: 0 {@code >}, 1
   * {@code >=}, 2 {@code <}, 3 {@code <=}, 4 {@code =}, 5 {@code !=}. Applied BEFORE top-K selection
   * (a filtered group must not occupy a window slot). v1 recognizes ONE such selection over a
   * post-group count let; anything else declines the pipeline.
   */
  public static final String GROUP_AGG_HAVING = "SIRIX_GROUP_AGG_HAVING";

  /**
   * A {@link PredicateNode} built HERE, over chain-qualified field names, for a {@code where} that
   * Brackit's own walker cannot represent: its leaves require a DIRECT {@code $r.field} deref, so a
   * nested {@code $r.commit.operation = "create"} leaves the whole pipeline unannotated. The leaves
   * are the SAME leaf kinds Brackit builds — only the field name is the column's relative path
   * ({@code "commit/operation"}), which is exactly what {@code Handle#columnOf} matches and what the
   * group keys and aggregate operands of this stage have always used. Set only when Brackit's
   * {@code VECTORIZED_PREDICATE_TREE} is ABSENT and every selection in the chain is representable;
   * the serving strategy prefers Brackit's tree and falls back to this one.
   */
  public static final String GROUP_AGG_PREDICATE = "SIRIX_GROUP_AGG_PREDICATE";

  /** Mirrors the kernel's packed-key bound (ProjectionIndexByteScan.MAX_GROUP_COLUMNS). */
  private static final int MAX_GROUP_KEYS = 5;

  /**
   * {@code -Dsirix.projDiag=true} prints ONE line per declined pipeline naming the first shape
   * element that failed. A decline is otherwise indistinguishable from "no fast path exists", which
   * costs a diagnosis cycle per formulation; the flag shares its name with the executor's and the
   * catalog's projection diagnostics so one switch reports the whole route.
   */
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");

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
      final String decline = tryAnnotate(node);
      if (decline != null && DIAG) {
        System.err.println("[groupagg-decline] " + decline);
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(node.getChild(i));
    }
  }

  /**
   * Try to annotate one pipeline.
   *
   * @return {@code null} when the pipeline was annotated OR is a shape this stage never claims beyond
   *         recognition (pure constant grouping annotates and returns {@code null} too); else the
   *         FIRST shape element that declined it, for {@code -Dsirix.projDiag} reporting
   */
  private String tryAnnotate(final AST pipeExpr) {
    if (pipeExpr.getChildCount() < 1) {
      return "pipe: no children";
    }
    final AST chain = pipeExpr.getChild(0);
    if (chain.getType() != XQ.Start || chain.getChildCount() < 1) {
      return "pipe: chain is not a Start node";
    }
    AST forBind = chain.getLastChild();
    while (forBind != null && forBind.getType() == XQ.LetBind) {
      forBind = forBind.getLastChild();
    }
    if (forBind == null || forBind.getType() != XQ.ForBind) {
      return "pipe: no ForBind at the chain head";
    }
    // Plain `for $r in src` only: `allowing empty` emits an empty-source tuple (one
    // group record over ZERO rows) and a positional `at $p` adds a variable — both
    // change row semantics. Decline EXPLICITLY rather than rely on the source-path
    // walker happening not to annotate shifted-child shapes.
    if (forBind.getChildCount() != 3 || forBind.getChild(0).getType() != XQ.TypedVariableBinding
        || forBind.getChild(1).getType() == XQ.AllowingEmpty
        || forBind.getChild(1).getType() == XQ.TypedVariableBinding) {
      return "for: not a plain `for $v in src` (allowing empty, positional at, or unexpected arity)";
    }
    final QNm loopVar = bindingVarName(forBind);
    if (loopVar == null) {
      return "for: loop variable is not a name";
    }
    final List<PreGroupLet> lets = new ArrayList<>();
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
    // The chain-aware predicate this stage builds itself, one conjunct per pre-group selection.
    // Nulled the moment a selection is not representable — the tree must cover the WHOLE where.
    List<PredicateNode> ownConjuncts = new ArrayList<>();
    AST current = forBind.getLastChild();
    for (; current != null && current.getType() != XQ.End; current = current.getLastChild()) {
      switch (current.getType()) {
        case XQ.Selection -> {
          // A selection AFTER group-by is HAVING-shaped. ONE `$countVar OP intLiteral`
          // over a post-group count let serves as a group filter (applied before top-K);
          // anything else declines — serving it as a ROW filter would change counts.
          if (hasGroupBy) {
            if (havingOpLit != null || !orderVars.isEmpty()) {
              return "having: a second post-group selection, or one after the order-by";
            }
            havingOpLit = havingCountFilter(current.getChild(0), postGroupVars, postGroupFuncs, postGroupFields);
            if (havingOpLit == null) {
              return "having: post-group where is not `$countLet OP intLiteral`";
            }
            continue;
          }
          // The predicate tree brackit annotates records FIELD names only, not deref
          // BASES — a where over some OTHER variable's field would be silently served
          // as a filter on the loop var. Require every variable in the selection to BE
          // the loop var.
          if (!onlyReferencesVar(current.getChild(0), loopVar)) {
            return "where: predicate references a variable other than the loop var";
          }
          hasSelection = true;
          // Pipeline semantics AND several selections together; one unrepresentable conjunct makes
          // the whole chain-aware tree unusable (never a partial filter).
          if (ownConjuncts != null) {
            final PredicateNode conjunct = chainPredicate(current.getChild(0), loopVar);
            if (conjunct == null) {
              ownConjuncts = null;
            } else {
              ownConjuncts.add(conjunct);
            }
          }
        }
        case XQ.LetBind -> {
          if (current.getChildCount() < 2) {
            return "let: binding node has no bound expression";
          }
          final QNm letVar = bindingVarName(current);
          // Shadowing declines, before and after the group-by alike: a re-bound var resolves
          // to its LAST binding in the interpreter while indexOf() would find the FIRST, and
          // a let shadowing the loop var changes every later deref.
          if (letVar == null || letVar.equals(loopVar) || indexOfLet(lets, letVar) >= 0
              || postGroupVars.contains(letVar) || constLetVars.contains(letVar)) {
            return "let: variable shadows the loop var or an earlier binding";
          }
          if (hasGroupBy) {
            // A POST-group let is servable only when it binds an AGGREGATE over the grouped
            // loop var — a value the scan already computes. After the group-by brackit binds
            // the loop var to the grouped SEQUENCE, so `count($r)` / `sum($r.f)` here mean
            // exactly what the same call means inside the return record. Anything else (a
            // per-group expression, a constant, a reference the scan never sees) declines.
            if (groupSpecVars.contains(letVar)) {
              return "let: post-group let rebinds a group-key variable";
            }
            final Agg plain = aggregateCall(current.getChild(1), loopVar, lets);
            // A SPAN let (`max(f) - min(f)`, or the whole-unit form the millisecond date_diff
            // idiom compiles to) is one aggregate over f, not a per-group expression: it reads the
            // min and max lanes the block already accumulates.
            final Agg agg = plain != null
                ? plain
                : spanCall(current.getChild(1), loopVar, lets, postGroupVars, postGroupFuncs, postGroupFields);
            if (agg == null) {
              return reason("let: post-group binding is neither an aggregate nor a span", current.getChild(1));
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
              lets.add(PreGroupLet.deref(letVar, field, 0L));
            } else if (bound.getType() == XQ.Int) {
              // A literal binding: a CONSTANT group key candidate (`let $g := 1`). Only a
              // group-by spec (and the record entry echoing it) may consume it.
              final long lit = intValue(bound);
              if (lit == Long.MIN_VALUE) {
                return "let: integer literal binding out of range";
              }
              constLetVars.add(letVar);
              constLetVals.add(lit);
            } else {
              final ShiftedDeref shifted = shiftedDeref(bound, loopVar);
              if (shifted != null) {
                lets.add(PreGroupLet.deref(letVar, shifted.field(), shifted.offset()));
                continue;
              }
              final String[] regex = regexReplaceCall(bound, loopVar, subField);
              if (regex != null) {
                lets.add(PreGroupLet.regex(letVar, subField[0], regex));
                continue;
              }
              // string-length($r.f): encoded as the "len:" field prefix — every later layer
              // (agg roster, accumulator blocks, ordering) treats "len:f" as its own operand.
              // fn:string-length(()) is 0, NOT empty: a row missing f contributes 0 to the
              // aggregate, which the kernel's strlen mode reproduces (fold 0, never skip).
              final String strlenField = strlenCall(bound, loopVar);
              if (strlenField != null) {
                lets.add(PreGroupLet.deref(letVar, "len:" + strlenField, 0L));
                continue;
              }
              // jn:utf8-length($r.f) is the byte-count twin used for SQL STRLEN. Keep its
              // operand distinct from fn:string-length: the projection kernel can compute either
              // from the same UTF-8 dictionary, but their answers differ for non-ASCII values.
              final String utf8LengthField = utf8LengthCall(bound, loopVar);
              if (utf8LengthField != null) {
                lets.add(PreGroupLet.deref(letVar, "utf8len:" + utf8LengthField, 0L));
                continue;
              }
              final CondDeref cond = conditionalDeref(bound, loopVar);
              if (cond != null) {
                lets.add(PreGroupLet.conditional(letVar, cond));
                continue;
              }
              // string($r.f) — the SQL-nullable-column port: fn:string(()) is "", so a row MISSING
              // the field joins (and prints as) the empty-string group rather than forming a group
              // with no key value. A pure EMISSION difference over the raw deref only when no
              // stored value is itself "", which is why the kernel hashes "" in the dict domain.
              final String stringifyField = stringCall(bound, loopVar);
              if (stringifyField != null) {
                lets.add(PreGroupLet.stringify(letVar, stringifyField));
                continue;
              }
              // ($r.f idiv D) mod M — integer date-part extraction (JSONBench's hour-of-day). The
              // kernel groups on the TRANSFORMED value, and Java's `/` and `%` are XQuery's `idiv`
              // and `mod` exactly (truncating division, dividend-signed remainder).
              final long[] divMod = divModDeref(bound, loopVar, subField);
              if (divMod != null) {
                lets.add(PreGroupLet.divMod(letVar, subField[0], divMod));
                continue;
              }
              int[] sub = integerOfSubstring(bound, loopVar, subField);
              int subKind = 0;
              if (sub == null) {
                sub = substringCall(bound, loopVar, subField);
                subKind = 1; // bare substring: a STRING-valued key transform
              }
              if (sub == null) {
                return reason("let: unmodelable pre-group binding", bound);
              }
              lets.add(PreGroupLet.substring(letVar, subField[0], new int[] {sub[0], sub[1], subKind}));
            }
          }
        }
        case XQ.OrderBy -> {
          // Only AFTER the group-by. A pre-group order by reorders the ROWS, which changes
          // which tuple is first in each group and therefore the group emission order the
          // served path reproduces — it is not a post-pass over the groups.
          if (!hasGroupBy || !orderVars.isEmpty()) {
            return "order by: before the group-by, or a second order-by";
          }
          for (int i = 0; i < current.getChildCount(); i++) {
            final AST spec = current.getChild(i);
            if (spec.getType() != XQ.OrderBySpec) {
              // The pipeline SUCCESSOR is a child of this node too (the chain is nested, not a
              // sibling list) — skip it exactly as the group-by arm above skips it.
              continue;
            }
            if (spec.getChildCount() < 1) {
              return "order by: spec has no key expression";
            }
            final AST key = spec.getChild(0);
            if (key.getType() != XQ.VariableRef || !(key.getValue() instanceof QNm orderVar)) {
              return reason("order by: key is not a bare variable reference", key); // only a bare variable — an
                                                                                    // expression would have to be
                                                                                    // re-evaluated
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
                return "order by: explicit collation"; // only the codepoint collation exists, and it is the default
                                                       // anyway
              } else {
                return "order by: unrecognised order modifier"; // an unrecognised modifier must never be silently
                                                                // dropped
              }
            }
            orderVars.add(orderVar);
            orderAsc.add(asc);
            orderEmptyLeast.add(emptyLeast);
          }
          if (orderVars.isEmpty()) {
            return "order by: no order-by spec found";
          }
        }
        case XQ.GroupBy -> {
          if (hasGroupBy) {
            // A SECOND group-by re-groups the grouped stream (and type-errors on >1-item
            // keys) — concatenating its specs into one joint grouping would replace that
            // error with an answer. Decline.
            return "group by: a second group-by";
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
              return "group by: spec is not a bare variable reference";
            }
            if (constLetVars.contains(var)) {
              constKeySpecs.add(var);
            } else {
              groupSpecVars.add(var);
            }
          }
        }
        default -> {
          return reason("pipeline: unsupported clause", current); // order-by / count / joins — not this shape
        }
      }
    }
    final int keyCount = groupSpecVars.size();
    // CONSTANT grouping: every key is a literal-bound let, the matching input is ONE group.
    // Mixed constant + real keys stay declined — dropping the constant from a real grouping is
    // sound but is not this shape.
    final boolean constMode = keyCount == 0 && !constKeySpecs.isEmpty();
    if (!hasGroupBy || current == null || current.getChildCount() < 1) {
      return "pipeline: no group-by, or no return expression";
    }
    if (!constMode && (keyCount < 1 || keyCount > MAX_GROUP_KEYS)) {
      return "group by: key count out of range";
    }
    if (constMode && !orderVars.isEmpty()) {
      // One group: an order-by is vacuous, but honoring it here would mean re-deriving Brackit's
      // single-tuple ordering semantics for no gain — fail closed.
      return "group by: constant-only grouping with an order-by";
    }
    // Duplicate group-spec vars (group by $d, $d) — degenerate; leave to the interpreter.
    if (new HashSet<>(groupSpecVars).size() != keyCount
        || new HashSet<>(constKeySpecs).size() != constKeySpecs.size()) {
      return "group by: duplicate group-key variable";
    }
    // Filter safety: a selection must never be served without a predicate the serving expression
    // will actually apply — the filter would silently vanish. Brackit's annotation is one such
    // predicate; the chain-aware tree built above is the other, for the nested-deref `where` its
    // direct-deref-only leaves cannot represent.
    PredicateNode ownPredicate = null;
    if (hasSelection && pipeExpr.getProperty(PREDICATE_TREE) == null) {
      if (ownConjuncts == null || ownConjuncts.isEmpty()) {
        return "where: selection is representable by neither Brackit's predicate tree nor a chain predicate";
      }
      ownPredicate = PredicateNode.and(ownConjuncts);
    }
    if (pipeExpr.getProperty(SOURCE_PATH) == null) {
      return "source: no VECTORIZED_SOURCE_PATH_PREFIX on the pipeline";
    }
    // Pure constant grouping (Q29) does NOT emit its keys — the record is aggregates only.
    // MIXED grouping (Q34) must emit every key, constant entries included.
    final int keyTotal = constMode
        ? 0
        : keyCount + constKeySpecs.size();
    final AST returnExpr = current.getChild(0);
    if (returnExpr == null || returnExpr.getType() != XQ.ObjectConstructor
        || returnExpr.getChildCount() < keyTotal + 1) {
      return reason("return: not an object constructor with an entry per key plus aggregates", returnExpr);
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
    final String[] keyRegexPattern = new String[keyCount];
    final String[] keyRegexRepl = new String[keyCount];
    final long[] keyDivMod = new long[2 * keyCount];
    final boolean[] keyStringify = new boolean[keyCount];
    boolean anyCondKey = false;
    boolean anyRegexKey = false;
    boolean anyDivModKey = false;
    boolean anyStringifyKey = false;
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
        return "return: key entry name missing or duplicated";
      }
      AST keyValue = keyEntry.getChild(1);
      String decorPrefix = null;
      String decorSuffix = null;
      if (keyValue.getType() == XQ.FunctionCall && keyValue.getChildCount() == 2
          && keyValue.getValue() instanceof QNm cfn && "concat".equals(cfn.getLocalName())) {
        final String cns = cfn.getNamespaceURI();
        if (cns != null && !cns.isEmpty() && !Namespaces.FN_NSURI.equals(cns)
            && !Namespaces.DEFAULT_FN_NSURI.equals(cns)) {
          return "return: concat over a non-builtin namespace";
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
          return "return: concat is not $key with a string literal";
        }
      }
      if (keyValue.getType() != XQ.VariableRef || !(keyValue.getValue() instanceof QNm keyVar)) {
        return reason("return: key entry value is not a variable reference", keyValue);
      }
      if (constKeySpecs.contains(keyVar)) {
        // A CONSTANT key entry: it partitions nothing — the expression splices the literal
        // back into every served record at this position.
        if (decorPrefix != null || decorSuffix != null || !seenConstVars.add(keyVar)) {
          return "return: constant key entry decorated or duplicated";
        }
        constEntryPos.add(i);
        constEntryNames.add(keyName);
        constEntryVals.add(constLetVals.get(constLetVars.indexOf(keyVar)));
        continue;
      }
      if (!groupSpecVars.contains(keyVar) || !seenGroupVars.add(keyVar)) {
        return "return: key entry variable is not a group key";
      }
      final int letIdx = indexOfLet(lets, keyVar);
      if (letIdx < 0) {
        return "return: group key has no pre-group let binding";
      }
      final PreGroupLet keyLet = lets.get(letIdx);
      realKeyVars[realIdx] = keyVar;
      keyNames[realIdx] = keyName;
      groupFields[realIdx] = keyLet.field();
      keyOffsets[realIdx] = keyLet.offset();
      final int[] sub = keyLet.substr();
      keySubstr[2 * realIdx] = sub == null
          ? 0
          : sub[2] == 1
              ? -sub[0]
              : sub[0];
      keySubstr[2 * realIdx + 1] = sub == null
          ? 0
          : sub[1];
      final String[] keyRegex = keyLet.regex();
      if (keyRegex != null) {
        keyRegexPattern[realIdx] = keyRegex[0];
        keyRegexRepl[realIdx] = keyRegex[1];
        anyRegexKey = true;
      }
      final CondDeref keyCond = keyLet.cond();
      if (keyCond != null) {
        keyCondFields[2 * realIdx] = keyCond.condField1();
        keyCondLits[2 * realIdx] = keyCond.condLit1();
        keyCondFields[2 * realIdx + 1] = keyCond.condField2();
        keyCondLits[2 * realIdx + 1] = keyCond.condLit2();
        keyCondElse[realIdx] = keyCond.elseLit();
        anyCondKey = true;
      }
      final long[] divMod = keyLet.divMod();
      if (divMod != null) {
        keyDivMod[2 * realIdx] = divMod[0];
        keyDivMod[2 * realIdx + 1] = divMod[1];
        anyDivModKey = true;
      }
      if (keyLet.stringify()) {
        keyStringify[realIdx] = true;
        anyStringifyKey = true;
      }
      // NOT keyLet.transformed(): a REGEX key is its own single-key arm, which declines outright
      // when another transform is present — folding it in here would decline every regex key.
      anyKeyTransform |=
          keyOffsets[realIdx] != 0L || sub != null || keyCond != null || divMod != null || keyLet.stringify();
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
      return "return: record does not echo every group key";
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
        return "return: aggregate entry name missing or duplicated";
      }
      final AST value = entry.getChild(1);
      final Agg agg;
      if (value.getType() == XQ.VariableRef && value.getValue() instanceof QNm valueVar) {
        // A reference to a post-group aggregate let: `let $c := count($r) ... return {"n": $c}`.
        final int postIdx = postGroupVars.indexOf(valueVar);
        if (postIdx < 0) {
          return "return: entry references a variable that is not a post-group aggregate";
        }
        agg = new Agg(postGroupFuncs.get(postIdx), postGroupFields.get(postIdx), postGroupOffsets.get(postIdx));
        emittedPostGroupVars.add(valueVar);
        emittedPostGroupAt.add(keyCount + i);
      } else {
        final Agg direct = aggregateCall(value, loopVar, lets);
        agg = direct != null
            ? direct
            : spanCall(value, loopVar, lets, postGroupVars, postGroupFuncs, postGroupFields);
        if (agg == null) {
          return reason("return: entry is neither an aggregate nor a span", value);
        }
      }
      if (agg.offset() != 0L && !constMode) {
        return "return: shifted aggregate operand outside constant grouping";
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
        return "order by: sorts on a value the record does not emit";
      }
      orderIndexes[i] = at;
      orderAscending[i] = orderAsc.get(i);
      orderEmptyLeastFlags[i] = orderEmptyLeast.get(i);
    }
    if (constMode && havingOpLit != null) {
      return "having: constant-only grouping with a HAVING filter"; // one group + HAVING: honoring it means maybe-empty
                                                                    // output — not this shape (v1)
    }
    if (constMode) {
      pipeExpr.setProperty(GROUP_AGG_CONST, Boolean.TRUE);
      if (ownPredicate != null) {
        pipeExpr.setProperty(GROUP_AGG_PREDICATE, ownPredicate);
      }
      pipeExpr.setProperty(GROUP_AGG_FUNCS, funcs);
      pipeExpr.setProperty(GROUP_AGG_FIELDS, fields);
      pipeExpr.setProperty(GROUP_AGG_OUT_NAMES, outNames);
      pipeExpr.setProperty(GROUP_AGG_OFFSETS, offsets);
      return null;
    }
    pipeExpr.setProperty(GROUP_AGG, Boolean.TRUE);
    if (ownPredicate != null) {
      pipeExpr.setProperty(GROUP_AGG_PREDICATE, ownPredicate);
    }
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
    if (anyDivModKey) {
      pipeExpr.setProperty(GROUP_AGG_KEY_DIVMOD, keyDivMod);
    }
    if (anyStringifyKey) {
      pipeExpr.setProperty(GROUP_AGG_KEY_STRINGIFY, keyStringify);
    }
    if (anyRegexKey) {
      if (keyCount > 1 || anyKeyTransform || anyCondKey) {
        return "key: regex transform combined with another key transform or several keys"; // the regex route is the
                                                                                           // single-string-key flat arm
                                                                                           // only (v1)
      }
      pipeExpr.setProperty(GROUP_AGG_KEY_REGEX_PATTERN, keyRegexPattern);
      pipeExpr.setProperty(GROUP_AGG_KEY_REGEX_REPL, keyRegexRepl);
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
    return null;
  }

  /**
   * A decline reason naming the offending subtree. Only the node's own label is included — a whole
   * subtree dump would be unreadable on a benchmark-sized FLWOR, and the label plus the shape element
   * is what identifies the formulation to change.
   */
  private static String reason(final String what, final AST node) {
    if (!DIAG) {
      return what;
    }
    return what + ": " + describe(node);
  }

  /** {@code Type[value]} for one AST node, bounds-checked against Brackit's name table. */
  private static String describe(final AST node) {
    if (node == null) {
      return "<none>";
    }
    final int type = node.getType();
    final String name = type >= 0 && type < XQ.NAMES.length
        ? XQ.NAMES[type]
        : "type#" + type;
    final Object value = node.getValue();
    if (value == null) {
      return name;
    }
    final String rendered = value instanceof QNm qnm
        ? qnm.getLocalName()
        : value.toString();
    return name + '[' + rendered + ']';
  }

  /**
   * Build a chain-aware {@link PredicateNode} for one {@code where} expression.
   *
   * <p>
   * Deliberately a SUBSET of Brackit's own extraction, leaf for leaf: {@code and}/{@code or}/parens
   * over comparisons of a loop-variable deref CHAIN against a string or integer literal. Every leaf
   * kind it produces ({@link PredicateNode.StrEq}, {@link PredicateNode.StrNe},
   * {@link PredicateNode.StrCmp}, {@link PredicateNode.NumCmp}) is one Brackit already produces for
   * the direct-deref spelling, so the executor's leaf handling — including its null-bearing-column
   * guards — applies unchanged; the only new thing is a field name that is a {@code '/'}-joined path.
   * Shapes Brackit claims but this does not ({@code fn:not}, {@code fn:contains}, array membership,
   * double/decimal literals, IN-list sequences, a bare boolean deref) return {@code null} and leave
   * the pipeline to the interpreter — they are only reachable when Brackit ALSO declined, which for
   * those shapes means a direct-deref query already serves.
   *
   * @return the predicate, or {@code null} when any part of it is not representable
   */
  private static PredicateNode chainPredicate(final AST expr, final QNm loopVar) {
    final AST node = unwrapParens(expr);
    if (node == null) {
      return null;
    }
    final int type = node.getType();
    if (type == XQ.AndExpr || type == XQ.OrExpr) {
      final int n = node.getChildCount();
      if (n < 2) {
        return null;
      }
      final List<PredicateNode> kids = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        final PredicateNode kid = chainPredicate(node.getChild(i), loopVar);
        if (kid == null) {
          return null;
        }
        kids.add(kid);
      }
      return type == XQ.AndExpr
          ? PredicateNode.and(kids)
          : PredicateNode.or(kids);
    }
    // Both spellings the parser produces: `ComparisonExpr(kind, left, right)` and the
    // kind-as-node-type form.
    final String op;
    final AST left;
    final AST right;
    if (type == XQ.ComparisonExpr && node.getChildCount() == 3) {
      op = comparisonOp(node.getChild(0).getType());
      left = node.getChild(1);
      right = node.getChild(2);
    } else if (comparisonOp(type) != null && node.getChildCount() == 2) {
      op = comparisonOp(type);
      left = node.getChild(0);
      right = node.getChild(1);
    } else {
      return null;
    }
    if (op == null) {
      return null;
    }
    final String leftField = loopVarDerefField(left, loopVar);
    if (leftField != null) {
      return comparisonLeaf(leftField, op, right);
    }
    final String rightField = loopVarDerefField(right, loopVar);
    // `"lit" lt $r.f` is `$r.f gt "lit"`: the ORDERING operators reverse, eq/ne are symmetric.
    return rightField == null
        ? null
        : comparisonLeaf(rightField, reverseComparisonOp(op), left);
  }

  /**
   * One comparison leaf over {@code field}, or {@code null} when {@code literal} is not a string or
   * integer literal this stage claims.
   */
  private static PredicateNode comparisonLeaf(final String field, final String op, final AST literal) {
    final AST lit = unwrapParens(literal);
    if (lit == null) {
      return null;
    }
    if (lit.getType() == XQ.Str) {
      final String value = lit.getValue() instanceof Str str
          ? str.stringValue()
          : lit.getValue() instanceof String s
              ? s
              : null;
      if (value == null) {
        return null;
      }
      return switch (op) {
        case "eq" -> new PredicateNode.StrEq(field, value);
        case "ne" -> new PredicateNode.StrNe(field, value);
        default -> new PredicateNode.StrCmp(field, op, value);
      };
    }
    if (lit.getType() == XQ.Int) {
      final long value = intValue(lit);
      // Long.MIN_VALUE is the "not usable" sentinel of intValue — declining it costs one
      // comparison literal and keeps every caller's range check in one place.
      return value == Long.MIN_VALUE
          ? null
          : new PredicateNode.NumCmp(field, op, value);
    }
    return null;
  }

  /**
   * Brackit's comparison-op tokens, for the general and the value comparison alike (over a
   * single-valued column they agree, and a missing operand is the empty sequence for both).
   */
  private static String comparisonOp(final int type) {
    return switch (type) {
      case XQ.GeneralCompEQ, XQ.ValueCompEQ -> "eq";
      case XQ.GeneralCompNE, XQ.ValueCompNE -> "ne";
      case XQ.GeneralCompGT, XQ.ValueCompGT -> "gt";
      case XQ.GeneralCompGE, XQ.ValueCompGE -> "ge";
      case XQ.GeneralCompLT, XQ.ValueCompLT -> "lt";
      case XQ.GeneralCompLE, XQ.ValueCompLE -> "le";
      default -> null;
    };
  }

  /**
   * Mirror of the relation for a literal-on-the-left comparison; {@code eq}/{@code ne} are symmetric
   * and pass through.
   */
  private static String reverseComparisonOp(final String op) {
    return switch (op) {
      case "gt" -> "lt";
      case "lt" -> "gt";
      case "ge" -> "le";
      case "le" -> "ge";
      default -> op;
    };
  }

  /**
   * One PRE-group {@code let} this stage modeled: the column it reads ({@code field}, a
   * {@code '/'}-joined chain) plus at most one transform applied to it. The transforms are mutually
   * exclusive by construction — each recognizer claims the whole bound expression — so a key carries
   * at most one, and {@link #transformed()} is the single rule an aggregate OPERAND is judged by (it
   * must read the raw column, and only the algebraic {@code offset} survives that).
   *
   * @param offset the constant a shifted deref added ({@code 0} for a plain one)
   * @param substr {@code xs:integer(substring(f, s, l))} / bare {@code substring} as
   *        {@code {start, length, kind}}, {@code kind == 1} marking the string-valued variant
   * @param cond the {@code if (...) then $r.f else "lit"} shape
   * @param regex {@code {pattern, replacement}} of a {@code replace($r.f, ...)} binding
   * @param divMod {@code {divisor, modulus}} of {@code ($r.f idiv D) mod M}; either may be 0 for "not
   *        applied", and both are positive when applied
   * @param stringify {@code string($r.f)}: a MISSING field reads as the empty string
   */
  private record PreGroupLet(QNm var, String field, long offset, int[] substr, CondDeref cond, String[] regex,
      long[] divMod, boolean stringify) {

    static PreGroupLet deref(final QNm var, final String field, final long offset) {
      return new PreGroupLet(var, field, offset, null, null, null, null, false);
    }

    static PreGroupLet substring(final QNm var, final String field, final int[] substr) {
      return new PreGroupLet(var, field, 0L, substr, null, null, null, false);
    }

    static PreGroupLet conditional(final QNm var, final CondDeref cond) {
      return new PreGroupLet(var, cond.thenField(), 0L, null, cond, null, null, false);
    }

    static PreGroupLet regex(final QNm var, final String field, final String[] regex) {
      return new PreGroupLet(var, field, 0L, null, null, regex, null, false);
    }

    static PreGroupLet divMod(final QNm var, final String field, final long[] divMod) {
      return new PreGroupLet(var, field, 0L, null, null, null, divMod, false);
    }

    static PreGroupLet stringify(final QNm var, final String field) {
      return new PreGroupLet(var, field, 0L, null, null, null, null, true);
    }

    /**
     * Bound to anything other than a plain or shifted deref — never a valid aggregate operand, which
     * would fold the RAW column the transform was supposed to replace.
     */
    boolean transformed() {
      return substr != null || cond != null || regex != null || divMod != null || stringify;
    }
  }

  /**
   * Index of the pre-group let binding {@code var}, or {@code -1}. Linear by design: a FLWOR carries
   * a handful of lets, and a map would allocate more than the scan costs.
   */
  private static int indexOfLet(final List<PreGroupLet> lets, final QNm var) {
    for (int i = 0; i < lets.size(); i++) {
      if (var.equals(lets.get(i).var())) {
        return i;
      }
    }
    return -1;
  }

  /**
   * A servable aggregate: {@code field} is {@code null} for {@code count}; {@code offset} is the
   * constant a shifted operand let added ({@code 0} for a plain deref).
   */
  private record Agg(String func, String field, long offset) {
  }

  /**
   * A pre-group let's shifted operand {@code $loop.field + k} / {@code k + $loop.field} /
   * {@code $loop.field - k}.
   */
  private record ShiftedDeref(String field, long offset) {
  }

  /**
   * A pre-group let's conditional key {@code if ($loop.c1 = L1 [and $loop.c2 = L2]) then
    * $loop.f else "lit"} — the SQL {@code CASE WHEN} port. {@code condField2} is {@code null} for a
   * single-conjunct condition.
   */
  private record CondDeref(String condField1, long condLit1, String condField2, long condLit2, String thenField,
      String elseLit) {
  }

  /**
   * Recognize the conditional key shape. Strict on purpose: the condition is one or two
   * {@code $loop.field = <int literal>} GENERAL comparisons (missing ⇒ false, the kernel's
   * existential), the then-branch a direct deref, the else-branch a string literal. Anything wider
   * (value comparisons, nested conditions, non-literal branches) declines the let.
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

  /**
   * {@code fn:replace($loop.field, 'pattern', 'replacement')} with LITERAL pattern and replacement
   * (built-in namespace, no flags argument) → {@code {pattern, replacement}} with the field in
   * {@code fieldOut[0]}; else {@code null}.
   */
  private static String[] regexReplaceCall(final AST expr, final QNm loopVar, final String[] fieldOut) {
    if (expr == null || expr.getType() != XQ.FunctionCall || expr.getChildCount() != 3
        || !(expr.getValue() instanceof QNm fn) || !"replace".equals(fn.getLocalName())) {
      return null;
    }
    final String ns = fn.getNamespaceURI();
    if (ns != null && !ns.isEmpty() && !Namespaces.FN_NSURI.equals(ns) && !Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
      return null;
    }
    final String field = loopVarDerefField(expr.getChild(0), loopVar);
    final AST pat = expr.getChild(1);
    final AST repl = expr.getChild(2);
    if (field == null || pat.getType() != XQ.Str || !(pat.getValue() instanceof Str p) || repl.getType() != XQ.Str
        || !(repl.getValue() instanceof Str r)) {
      return null;
    }
    fieldOut[0] = field;
    return new String[] {p.stringValue(), r.stringValue()};
  }

  /**
   * {@code fn:string($loop.field)} (built-in namespace, one argument) → the field's chain, else
   * {@code null}.
   *
   * <p>
   * The zero-argument {@code fn:string()} reads the context item and is a different function; the
   * {@code xs:string(...)} CONSTRUCTOR is deliberately not claimed — it raises on a missing operand
   * ({@code xs:string(())} is the empty sequence, not {@code ""}) where {@code fn:string}
   * substitutes, and the whole point of the transform is that substitution.
   */
  private static String stringCall(final AST expr, final QNm loopVar) {
    if (expr == null || expr.getType() != XQ.FunctionCall || expr.getChildCount() != 1
        || !(expr.getValue() instanceof QNm fn) || !"string".equals(fn.getLocalName())) {
      return null;
    }
    final String ns = fn.getNamespaceURI();
    if (ns != null && !ns.isEmpty() && !Namespaces.FN_NSURI.equals(ns) && !Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
      return null;
    }
    return loopVarDerefField(expr.getChild(0), loopVar);
  }

  /**
   * {@code ($loop.f idiv D) mod M}, {@code $loop.f idiv D} or {@code $loop.f mod M} with POSITIVE
   * integer literals → {@code {divisor, modulus}} ({@code 0} for an absent operation), with the
   * field's chain in {@code fieldOut[0]}; else {@code null}.
   *
   * <p>
   * Positive literals only, and that is what makes the transform exact rather than approximate:
   * Java's {@code /} truncates toward zero exactly as XQuery's {@code idiv} does, and Java's
   * {@code %} takes the dividend's sign exactly as XQuery's {@code mod} does, so the kernel computes
   * the interpreter's value for EVERY input including negatives. A zero divisor or modulus raises in
   * the interpreter (FOAR0001) and a negative one is simply not a date-part extraction — both decline
   * instead of being reproduced.
   */
  private static long[] divModDeref(final AST expr, final QNm loopVar, final String[] fieldOut) {
    final AST outer = unwrapParens(expr);
    if (outer == null || outer.getType() != XQ.ArithmeticExpr || outer.getChildCount() != 3) {
      return null;
    }
    final int outerOp = outer.getChild(0).getType();
    if (outerOp != XQ.ModulusOp && outerOp != XQ.IDivideOp) {
      return null;
    }
    final long outerLit = intValue(outer.getChild(2));
    if (outerLit < 1L) {
      return null;
    }
    final AST inner = unwrapParens(outer.getChild(1));
    if (inner == null) {
      return null;
    }
    // The one-operation forms: `$r.f idiv D` and `$r.f mod M`.
    final String directField = loopVarDerefField(inner, loopVar);
    if (directField != null) {
      fieldOut[0] = directField;
      return outerOp == XQ.IDivideOp
          ? new long[] {outerLit, 0L}
          : new long[] {0L, outerLit};
    }
    // The composed form, in the one order that is a date-part extraction: (f idiv D) mod M.
    if (outerOp != XQ.ModulusOp || inner.getType() != XQ.ArithmeticExpr || inner.getChildCount() != 3
        || inner.getChild(0).getType() != XQ.IDivideOp) {
      return null;
    }
    final long divisor = intValue(inner.getChild(2));
    if (divisor < 1L) {
      return null;
    }
    final String field = loopVarDerefField(unwrapParens(inner.getChild(1)), loopVar);
    if (field == null) {
      return null;
    }
    fieldOut[0] = field;
    return new long[] {divisor, outerLit};
  }

  /**
   * {@code fn:string-length($loop.field)} (built-in namespace only) → the field name, else
   * {@code null}.
   */
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

  /** {@code jn:utf8-length($loop.field)} → the field name, else {@code null}. */
  private static String utf8LengthCall(final AST expr, final QNm loopVar) {
    if (expr == null || expr.getType() != XQ.FunctionCall || expr.getChildCount() != 1
        || !(expr.getValue() instanceof QNm fn) || !"utf8-length".equals(fn.getLocalName())
        || !JSONFun.JSON_NSURI.equals(fn.getNamespaceURI())) {
      return null;
    }
    return loopVarDerefField(expr.getChild(0), loopVar);
  }

  /**
   * {@code $countVar OP <int literal>} (either operand order, general comparisons only) where
   * {@code $countVar} is a post-group {@code count($loop)} let → {@code long[]{op, literal}} under
   * {@link #GROUP_AGG_HAVING}'s encoding; else {@code null}. Counts are the only v1 operand: every
   * other aggregate's lane may be absent for a group (empty min over a missing field), and a filter
   * over an empty operand needs the interpreter's comparison semantics.
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
      if (lit == Long.MIN_VALUE || varSide.getType() != XQ.VariableRef || !(varSide.getValue() instanceof QNm var)) {
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

  /**
   * {@code $loop.field = <int literal>} (either operand order, general {@code =} only) → the field
   * name, with the literal in {@code litOut[0]}; else {@code null}.
   */
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
   * SHIFTED deref ({@code $r.f + k}) carries its offset; only the constant-grouping route may consume
   * a non-zero one.
   *
   * @return the aggregate, or {@code null} when the expression is not servable
   */
  private static Agg aggregateCall(final AST call, final QNm loopVar, final List<PreGroupLet> lets) {
    if (call == null || call.getType() != XQ.FunctionCall || call.getChildCount() != 1
        || !(call.getValue() instanceof QNm fn)) {
      return null;
    }
    // xs:double(<aggregate>) — SQL AVG is a double, so every ported benchmark wraps avg this way,
    // and the wrap must not decline the whole pipeline. The token is PREFIXED ("dbl:avg") so the
    // cast flows through every annotation layer unchanged; emission applies Brackit's own cast to
    // the exact value, digit-for-digit what the interpreter's constructor function computes.
    if (Namespaces.XS_NSURI.equals(fn.getNamespaceURI()) && "double".equals(fn.getLocalName())) {
      final Agg inner = aggregateCall(call.getChild(0), loopVar, lets);
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
        if (ins == null || ins.isEmpty() || Namespaces.FN_NSURI.equals(ins)
            || Namespaces.DEFAULT_FN_NSURI.equals(ins)) {
          final AST dArg = arg.getChild(0);
          final String direct = loopVarDerefField(dArg, loopVar);
          if (direct != null) {
            return new Agg("count-distinct", direct, 0L);
          }
          if (dArg.getType() == XQ.VariableRef && dArg.getValue() instanceof QNm dv) {
            final int li = indexOfLet(lets, dv);
            if (li >= 0 && lets.get(li).offset() == 0L && !lets.get(li).transformed()) {
              // A SHIFTED let stays declined: distinct-count is shift-invariant in principle,
              // but proving that here buys nothing ClickBench-shaped.
              return new Agg("count-distinct", lets.get(li).field(), 0L);
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
      final int letIdx = indexOfLet(lets, argVar);
      if (letIdx >= 0 && !lets.get(letIdx).transformed()) {
        // A substring- or conditionally-transformed let as an operand would fold the RAW column.
        return new Agg(func, lets.get(letIdx).field(), lets.get(letIdx).offset());
      }
    }
    return null;
  }

  /**
   * A post-group SPAN: {@code max($r.f) - min($r.f)}, or the whole-unit difference
   * {@code (max($r.f) idiv k) - (min($r.f) idiv k)} that a millisecond {@code date_diff} over
   * microsecond timestamps compiles to. Both sides must read the SAME field through the SAME positive
   * integer divisor; either side may be a reference to a post-group {@code max}/{@code min} let,
   * which is how the idiom is normally written.
   *
   * <p>
   * Encoded as the func token {@code "span:k"} — one aggregate over {@code f}, reading the min and
   * max lanes its accumulator block already carries, adding no lane and no scan cost. The divisor
   * rides in the token rather than in {@link Agg#offset()}, which the grouped route rejects outright.
   *
   * <p>
   * {@code (max idiv k) - (min idiv k)} is deliberately NOT rewritten to {@code (max - min) idiv k}:
   * the two differ by up to one unit, and it is the former that {@code dateDiff} defines.
   *
   * @return the span aggregate, or {@code null} when the expression is not one
   */
  private static Agg spanCall(final AST expr, final QNm loopVar, final List<PreGroupLet> lets,
      final List<QNm> postGroupVars, final List<String> postGroupFuncs, final List<String> postGroupFields) {
    final AST diff = unwrapParens(expr);
    if (diff == null || diff.getType() != XQ.ArithmeticExpr || diff.getChildCount() != 3
        || diff.getChild(0).getType() != XQ.SubtractOp) {
      return null;
    }
    final long[] maxDiv = new long[1];
    final long[] minDiv = new long[1];
    final String maxField =
        spanOperand(diff.getChild(1), "max", maxDiv, loopVar, lets, postGroupVars, postGroupFuncs, postGroupFields);
    if (maxField == null) {
      return null;
    }
    final String minField =
        spanOperand(diff.getChild(2), "min", minDiv, loopVar, lets, postGroupVars, postGroupFuncs, postGroupFields);
    // A different operand on each side is a different quantity entirely, and two different divisors
    // do not reduce to one scaled span — both decline instead of serving an approximation.
    if (minField == null || !maxField.equals(minField) || maxDiv[0] != minDiv[0]) {
      return null;
    }
    return new Agg("span:" + maxDiv[0], maxField, 0L);
  }

  /**
   * One side of a {@link #spanCall}: {@code <agg>} or {@code <agg> idiv k}, where {@code <agg>} is
   * {@code wantFunc($loop.field)} or a reference to a post-group let binding exactly that. The
   * divisor lands in {@code divOut[0]} ({@code 1} when there is none).
   *
   * @return the operand field, or {@code null} when this side is not the wanted aggregate
   */
  private static String spanOperand(final AST operand, final String wantFunc, final long[] divOut, final QNm loopVar,
      final List<PreGroupLet> lets, final List<QNm> postGroupVars, final List<String> postGroupFuncs,
      final List<String> postGroupFields) {
    AST side = unwrapParens(operand);
    if (side == null) {
      return null;
    }
    long div = 1L;
    if (side.getType() == XQ.ArithmeticExpr && side.getChildCount() == 3
        && side.getChild(0).getType() == XQ.IDivideOp) {
      div = intValue(side.getChild(2));
      if (div <= 0L) {
        // Non-literal, zero or negative: zero raises, a negative divisor REVERSES the order the
        // in-kernel comparator would produce. Neither is servable by scaling the raw span.
        return null;
      }
      side = unwrapParens(side.getChild(1));
      if (side == null) {
        return null;
      }
    }
    divOut[0] = div;
    if (side.getType() == XQ.VariableRef && side.getValue() instanceof QNm ref) {
      final int at = postGroupVars.indexOf(ref);
      // A pre-group let reference is NOT this: it names a per-row value, not a per-group extremum.
      return at >= 0 && wantFunc.equals(postGroupFuncs.get(at))
          ? postGroupFields.get(at)
          : null;
    }
    final Agg agg = aggregateCall(side, loopVar, lets);
    // A shifted operand would need the shift folded into both sides; a cast wrap (dbl:max) rounds.
    return agg != null && wantFunc.equals(agg.func()) && agg.field() != null && agg.offset() == 0L
        ? agg.field()
        : null;
  }

  /** Strip transparent single-child {@code ParenthesizedExpr} wraps the parser keeps until later. */
  private static AST unwrapParens(final AST expr) {
    AST e = expr;
    while (e != null && e.getType() == XQ.ParenthesizedExpr && e.getChildCount() == 1) {
      e = e.getChild(0);
    }
    return e;
  }

  /**
   * {@code $loopVar.field + k}, {@code k + $loopVar.field} or {@code $loopVar.field - k} with an
   * INTEGER literal — the shifted-operand shape {@code sum(f+k) = sum(f) + k·presentCount(f)} serves
   * algebraically. Anything else (doubles, nested arithmetic, two derefs) returns {@code null}.
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
   * {@code xs:integer(substring($loopVar.f, startLit, lenLit))} — the DATE_TRUNC/EXTRACT idiom over
   * ISO-8601 strings. Positive int literals only; the field lands in {@code fieldOut[0]}.
   */
  private static int[] integerOfSubstring(final AST expr, final QNm loopVar, final String[] fieldOut) {
    if (expr == null || expr.getType() != XQ.FunctionCall || expr.getChildCount() != 1
        || !(expr.getValue() instanceof QNm outer) || !"integer".equals(outer.getLocalName())
        || !Namespaces.XS_NSURI.equals(outer.getNamespaceURI())) {
      return null;
    }
    return substringCall(expr.getChild(0), loopVar, fieldOut);
  }

  /**
   * The bare {@code substring($loopVar.f, startLit, lenLit)} — the DATE_TRUNC idiom's STRING form.
   * Positive int literals only; the field lands in {@code fieldOut[0]}.
   */
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

  /**
   * Value of an integer literal node, or {@code Long.MIN_VALUE} when it is not usable (that sentinel
   * itself would negate to an overflow, so it declines too).
   */
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

  /**
   * Longest deref chain this recognizes. Deeper is not unservable in principle — no projection
   * declares columns that deep, and the bound keeps the recursion below trivially bounded.
   */
  private static final int MAX_DEREF_DEPTH = 8;

  /**
   * A PURE deref chain rooted at the loop variable — {@code $loopVar.field} or
   * {@code $loopVar.a.b.field} — as the chain of field steps joined by {@code '/'}, i.e. the column's
   * path RELATIVE to the record root ({@code "field"}, {@code "a/b/field"}).
   *
   * <p>
   * The single-step form returns exactly what it always did (a bare field name), so every caller's
   * existing behavior is unchanged. The multi-step form is what lets a nested column be named at all;
   * the relative chain — not the trailing name — is what the covering lookup and
   * {@code Handle#columnOf} match against, so {@code $r.commit.collection} can never be answered from
   * a top-level {@code collection} column, nor {@code $r.collection} from a nested one.
   *
   * <p>
   * Every step must be a deref with a STRING/QNm key: a computed key, an array step, a function call
   * or any other base declines the whole chain (returns {@code null}), exactly as a non-deref did
   * before. A key containing {@code '/'} declines too — it would forge a chain separator, and no
   * projection column can carry such a name (column paths are {@code '/'}-separated).
   */
  private static String loopVarDerefField(final AST expr, final QNm loopVar) {
    return loopVarDerefField(expr, loopVar, MAX_DEREF_DEPTH);
  }

  private static String loopVarDerefField(final AST expr, final QNm loopVar, final int depthLeft) {
    if (depthLeft <= 0 || expr == null || expr.getType() != XQ.DerefExpr || expr.getChildCount() < 2) {
      return null;
    }
    final String field = derefStepName(expr);
    if (field == null) {
      return null;
    }
    final AST base = expr.getChild(0);
    if (base.getType() == XQ.VariableRef) {
      return loopVar.equals(base.getValue())
          ? field
          : null;
    }
    final String prefix = loopVarDerefField(base, loopVar, depthLeft - 1);
    return prefix == null
        ? null
        : prefix + '/' + field;
  }

  /** The field name a single deref step selects, or {@code null} when it is not a literal key. */
  private static String derefStepName(final AST deref) {
    final Object name = deref.getChild(deref.getChildCount() - 1).getValue();
    final String local;
    if (name instanceof QNm qnm) {
      local = qnm.getLocalName();
    } else if (name instanceof String s) {
      local = s;
    } else {
      return null;
    }
    return local == null || local.indexOf('/') >= 0
        ? null
        : local;
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
