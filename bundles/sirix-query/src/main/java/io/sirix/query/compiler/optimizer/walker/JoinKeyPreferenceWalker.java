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
 * input, so they filter the build side before the join;</li>
 * <li>the equality nearest the head of the chain that {@code JoinRewriter} would accept as a join
 * condition — the join key. Acceptance is the test {@code JoinRewriter} itself applies, so this is
 * the very equality it would reach first, and whenever its own choice is already an equality the key
 * is left exactly as it was: the rule only ever re-keys away from a non-equality, never from one
 * equality to another. That includes an equality with a mixed-side operand, as in
 * {@code $c.k eq $p.a + $q.b}, which {@code JoinRewriter} keys on by building the mixed side on the
 * build input — re-keying off it would enumerate the probe side's cross product instead;</li>
 * <li>every remaining predicate (other equalities, inequalities, mixed predicates) in its chain
 * order — they follow the join as residual filters, which Brackit's {@code PredicateMerge} then
 * collapses into one conjunction.</li>
 * </ol>
 * Selections referencing only earlier bindings were already lifted above this binding by
 * {@code SelectPullup}. A chain without an eligible equality is left untouched, so inequality-only
 * joins keep their current plan. Selections are filters, so reordering them changes no answer.
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
      switch (classify(selection.getChild(0), bind)) {
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
   * {@link #EQUALITY} mirrors {@code JoinRewriter}'s eligibility for an equality it can key on: both
   * sides non-static, and one side referencing a strictly earlier binding than the other. That is
   * what its scope split tests — it orients the two operands so the one reaching the later binding
   * builds, then requires the other to begin earlier — so every equality it would key on, including
   * one whose single operand mixes bindings, lands here rather than in {@link #RESIDUAL}.
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
  private int classify(final AST predicate, final Scope bind) {
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
    final Scope left = earliestBinding(predicate.getChild(1));
    final Scope right = earliestBinding(predicate.getChild(2));
    if (left == null || right == null) {
      return RESIDUAL;
    }
    return left.compareTo(right) == 0
        ? RESIDUAL
        : EQUALITY;
  }

  /**
   * The earliest pipeline binding one comparison side references, or {@code null} when the side is
   * static — {@code JoinRewriter} does not join on a static side.
   */
  private Scope earliestBinding(final AST expression) {
    final VarRef refs = findVarRefs(expression);
    if (refs == null) {
      return null;
    }
    final Scope[] scopes = sortScopes(refs);
    return scopes.length == 0
        ? null
        : scopes[0];
  }
}
