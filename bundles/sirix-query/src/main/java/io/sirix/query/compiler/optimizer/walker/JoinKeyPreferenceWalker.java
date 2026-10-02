package io.sirix.query.compiler.optimizer.walker;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.topdown.ScopeWalker;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Orders the selection chain below a {@code for} or {@code let} binding so that Brackit's join
 * recognition keys the join on an equality and keeps every other predicate where it costs least.
 *
 * <p>
 * Brackit's {@code SelectPullup} lifts every selection whose innermost dependency is the binding to
 * directly below that binding, one on top of the other, so the resulting chain no longer follows
 * the textual order of the where clause; its top-down {@code JoinRewriter} then turns the
 * <em>first</em> join-capable comparison in that chain into the join condition. For
 * {@code where $c.pid eq $p.id
 * and ... and xs:dateTime($p.vf) lt xs:dateTime($c.vt)} the head of the chain is the trailing
 * inequality: a general-comparison join that emits close to the cross product and evaluates the
 * equality per pair.
 * </p>
 *
 * <p>
 * This walker runs between those two and rewrites each chain of selections directly below a binding
 * into
 * <ol>
 * <li>the selections that reference no earlier binding — Brackit copies them into the join's right
 * input, so they filter the build side before the join. That widens what such a predicate sees: the
 * build side is drained in full, so a predicate hoisted here is evaluated on rows that never join,
 * and one that raises a dynamic error on such a row now raises it for the whole query;</li>
 * <li>the equality nearest the head of the chain that {@code JoinRewriter} would key on and whose
 * plan compiles — the join key. The test is {@code JoinRewriter}'s own, so this is the very
 * equality it would reach first, and whenever its own choice already qualifies the key is left
 * exactly as it was: the rule re-keys only away from a non-equality, or away from an equality whose
 * plan does not compile, never from one qualifying equality to another. A mixed operand is welcome
 * on the build side, as in {@code $c.k eq $p.a + $q.b}, which {@code JoinRewriter} evaluates on the
 * build input — re-keying off it would enumerate the probe side's cross product instead. On the
 * probe side it is not: that operand is compiled above the build binding, where it cannot read it.
 * Either way an equality that does not qualify stays a residual rather than being hoisted, since
 * hoisting it would only copy it into the join's right input to be re-evaluated for every tuple of
 * the enclosing binding;</li>
 * <li>every remaining predicate (other equalities, inequalities, mixed predicates) in its chain
 * order — they follow the join as residual filters, which Brackit's {@code PredicateMerge} then
 * collapses into one conjunction.</li>
 * </ol>
 * Selections referencing only earlier bindings were already lifted above this binding by
 * {@code SelectPullup}. A chain without an eligible equality is left untouched, so inequality-only
 * joins keep their current plan.
 * </p>
 *
 * <p>
 * Selections are filters, so reordering them yields the same result set. It does not preserve
 * dynamic errors: each moved predicate is evaluated over a different set of rows. A single-side
 * conjunct hoisted to the build side (1) sees rows that never join and can raise where the query
 * used to answer; a predicate demoted to the residual (3) sees only joined pairs and can stop
 * raising where the query used to fail. XQuery leaves the evaluation order of where-clause
 * conjuncts to the implementation, so either outcome is permitted, and Brackit already exposes the
 * first one whenever {@code SelectPullup} happens to leave a single-side conjunct above the key.
 * </p>
 */
public final class JoinKeyPreferenceWalker extends ScopeWalker {

  private static final int RIGHT_ONLY = 0;
  private static final int EQUALITY = 1;
  private static final int RESIDUAL = 2;

  public JoinKeyPreferenceWalker(final StaticContext sctx) {
    super(sctx);
  }

  @Override
  protected AST visit(final AST node) {
    final int type = node.getType();
    if (type != XQ.ForBind && type != XQ.LetBind) {
      return node;
    }
    final AST first = node.getLastChild();
    if (first == null || first.getType() != XQ.Selection || first.getLastChild() == null
        || first.getLastChild().getType() != XQ.Selection) {
      return node;
    }
    final Scope bind = findScope(node);
    if (bind == null || bind.getNode() != node) {
      return node;
    }
    final List<AST> chain = new ArrayList<>(8);
    AST current = first;
    while (current != null && current.getType() == XQ.Selection) {
      chain.add(current);
      current = current.getLastChild();
    }
    if (chain.size() < 2 || current == null) {
      return node;
    }
    final AST downstream = current;
    final List<AST> rightOnly = new ArrayList<>(chain.size());
    final List<AST> residual = new ArrayList<>(chain.size());
    AST key = null;
    for (final AST selection : chain) {
      switch (classify(selection, bind)) {
        case RIGHT_ONLY -> rightOnly.add(selection);
        case EQUALITY -> {
          if (key == null) {
            key = selection;
          } else {
            residual.add(selection);
          }
        }
        default -> residual.add(selection);
      }
    }
    if (key == null) {
      return node;
    }
    final List<AST> order = new ArrayList<>(chain.size());
    order.addAll(rightOnly);
    order.add(key);
    order.addAll(residual);
    if (order.equals(chain)) {
      return node;
    }
    AST next = downstream;
    for (int i = order.size() - 1; i >= 0; i--) {
      final AST selection = order.get(i);
      selection.replaceChild(selection.getChildCount() - 1, next);
      next = selection;
    }
    node.replaceChild(node.getChildCount() - 1, next);
    snapshot();
    refreshScopes(node, true);
    return node;
  }

  /**
   * {@link #EQUALITY} is an equality this walker may move to the head of the chain. It applies
   * {@code JoinRewriter}'s own admission test, so the key is the very predicate {@code JoinRewriter}
   * would key on: both operands non-static; the operand reaching the later binding builds, the
   * written order kept when both reach the same one; the probe operand must begin at a strictly
   * earlier binding than the build operand; and the build operand's first binding must enclose the
   * selection in this pipeline. The orientation step is why admission depends on the written order.
   *
   * <p>
   * Admission alone is not enough. {@code convertToJoin} compiles the probe operand into a left input
   * rooted above the build binding without checking that it can be evaluated there, so an equality
   * whose probe operand also reads the build binding — {@code $c.cost + $p.retail eq
   * $p.retail + 5}, whose operands tie on {@code $p} and so keep their written order — yields a left
   * input where {@code $p} is unbound and no plan at all. Every pipeline binding the probe operand
   * reads must therefore precede the build root. Declining to promote such an equality never makes a
   * working plan worse: below the head it stays the residual it already was, and at the head — where
   * {@code JoinRewriter} would otherwise key on it and emit that unresolvable left input — the rule
   * keys on a later qualifying equality instead, so a plan that does not compile is replaced by one
   * that does.
   * </p>
   *
   * <p>
   * {@link #RIGHT_ONLY}: the predicate references no earlier binding, so it filters this binding's
   * input on its own. Everything else is {@link #RESIDUAL}.
   *
   * <p>
   * Only an earlier pipeline binding compares less than {@code bind}. The scopes {@code ScopeWalker}
   * opens inside a predicate — a quantified binding, a filter's {@code fs:dot}, a nested FLWOR — are
   * descendants of {@code bind} and compare greater, so it is the sign of {@code Scope.compareTo},
   * not inequality with {@code bind}, that separates a foreign binding from the predicate's own
   * scopes.
   * </p>
   */
  private int classify(final AST selection, final Scope bind) {
    final AST predicate = selection.getChild(0);
    final VarRef refs = findVarRefs(predicate);
    if (refs == null) {
      return RIGHT_ONLY;
    }
    final Scope[] scopes = sortScopes(refs);
    boolean usesEarlier = false;
    for (final Scope scope : scopes) {
      if (scope.compareTo(bind) < 0) {
        usesEarlier = true;
        break;
      }
    }
    if (!usesEarlier) {
      return RIGHT_ONLY;
    }
    if (predicate.getType() != XQ.ComparisonExpr || predicate.getChildCount() != 3) {
      return RESIDUAL;
    }
    final int comparison = predicate.getChild(0).getType();
    if (comparison != XQ.GeneralCompEQ && comparison != XQ.ValueCompEQ) {
      return RESIDUAL;
    }
    final Scope[] written = operandScopes(predicate.getChild(1));
    final Scope[] other = operandScopes(predicate.getChild(2));
    if (written == null || other == null) {
      return RESIDUAL;
    }
    final boolean swap = other[other.length - 1].compareTo(written[written.length - 1]) < 0;
    final Scope[] probe = swap
        ? other
        : written;
    final Scope[] build = swap
        ? written
        : other;
    return probe[0].compareTo(build[0]) < 0 && probeBoundAbove(probe, bind, build[0])
        && buildRootEncloses(selection, build[0])
            ? EQUALITY
            : RESIDUAL;
  }

  /**
   * The pipeline scopes one comparison operand references, earliest first, or {@code null} when the
   * operand is static — {@code JoinRewriter} does not join on a static operand.
   */
  private Scope[] operandScopes(final AST expression) {
    final VarRef refs = findVarRefs(expression);
    if (refs == null) {
      return null;
    }
    final Scope[] scopes = sortScopes(refs);
    return scopes.length == 0
        ? null
        : scopes;
  }

  /**
   * Whether every pipeline binding the probe operand reads is bound above {@code buildRoot}, the root
   * of the join's right input. The scopes a predicate opens itself sort after {@code bind} and need
   * no pipeline binding, so the latest scope at or above {@code bind} is the one that has to precede
   * {@code buildRoot}; an operand reading only its own scopes imposes nothing.
   */
  private static boolean probeBoundAbove(final Scope[] probe, final Scope bind, final Scope buildRoot) {
    for (int i = probe.length - 1; i >= 0; i--) {
      final Scope scope = probe[i];
      if (scope.compareTo(bind) <= 0) {
        return scope.compareTo(buildRoot) < 0;
      }
    }
    return true;
  }

  /**
   * Whether {@code buildRoot} binds above {@code selection} in this pipeline, which is where
   * {@code JoinRewriter} roots the join's right input. It walks the same ancestors and stops at the
   * same clause boundaries, so a scope the predicate opens itself — a descendant, never an ancestor —
   * and one cut off by a {@code GroupBy}, {@code OrderBy} or {@code Count} both fail here exactly as
   * they make {@code JoinRewriter} leave the selection alone.
   */
  private static boolean buildRootEncloses(final AST selection, final Scope buildRoot) {
    final AST root = buildRoot.getNode();
    for (AST parent = selection.getParent(); parent != null; parent = parent.getParent()) {
      final int type = parent.getType();
      if (type == XQ.Start || type == XQ.GroupBy || type == XQ.OrderBy || type == XQ.Count) {
        return false;
      }
      if (parent == root) {
        return true;
      }
    }
    return false;
  }
}
