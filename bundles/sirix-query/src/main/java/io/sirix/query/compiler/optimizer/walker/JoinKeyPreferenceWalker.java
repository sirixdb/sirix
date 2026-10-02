package io.sirix.query.compiler.optimizer.walker;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.topdown.ScopeWalker;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Orders the selection chain below a {@code for} binding so that Brackit's join recognition keys the
 * join on an equality and keeps every other predicate where it costs least.
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
 * <li>the equality nearest the head of the chain whose two sides reference this binding and only
 * earlier bindings, respectively — the join key. That is the first equality of this separated-sides
 * class {@code JoinRewriter} itself reaches, so whenever its own choice already belongs to the class
 * the key is left exactly as it was. An equality whose one side mixes this binding with an earlier
 * one, as in {@code $c.qty * $p.price eq $p.total}, does not belong to the class:
 * {@code JoinRewriter} would key on such a comparison, this rule makes it a residual and keys on a
 * separated equality instead. Nothing is lost by that — keying on it puts a key expression
 * referencing this binding on the probe side, which Brackit's own plan then cannot resolve;</li>
 * <li>every remaining predicate (other equalities, inequalities, mixed predicates) in its chain
 * order — they follow the join as residual filters, which Brackit's {@code PredicateMerge} then
 * collapses into one conjunction.</li>
 * </ol>
 * Selections referencing only earlier bindings were already lifted above this binding by
 * {@code SelectPullup}. A chain without an eligible equality is left untouched, so inequality-only
 * joins keep their current plan, and so does every {@code let}-bound chain. Selections are filters,
 * so reordering them changes no answer.
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
    if (node.getType() != XQ.ForBind) {
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
   * {@link #EQUALITY} mirrors {@code JoinRewriter}'s eligibility for an equality keyed on this
   * binding: a comparison whose one side references only this binding and whose other side references
   * only earlier bindings, both sides non-static. {@link #RIGHT_ONLY}: the predicate references no
   * earlier binding, so it filters this binding's input on its own. Everything else is
   * {@link #RESIDUAL}.
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
    final int left = side(predicate.getChild(1), bind);
    final int right = side(predicate.getChild(2), bind);
    final boolean separated =
        (left == SIDE_THIS && right == SIDE_EARLIER) || (left == SIDE_EARLIER && right == SIDE_THIS);
    return separated
        ? EQUALITY
        : RESIDUAL;
  }

  private static final int SIDE_THIS = 1;
  private static final int SIDE_EARLIER = 2;
  private static final int SIDE_OTHER = 3;

  /**
   * Which pipeline bindings one comparison side references: only this one, only earlier ones, or a
   * mix. A scope opened inside the predicate itself is a descendant of {@code bind} and so belongs to
   * this binding's side.
   */
  private int side(final AST expression, final Scope bind) {
    final VarRef refs = findVarRefs(expression);
    if (refs == null) {
      return SIDE_OTHER; // static side: JoinRewriter does not join on it
    }
    boolean thisBinding = false;
    boolean earlier = false;
    for (final Scope scope : sortScopes(refs)) {
      if (scope.compareTo(bind) < 0) {
        earlier = true;
      } else {
        thisBinding = true;
      }
    }
    if (thisBinding && !earlier) {
      return SIDE_THIS;
    }
    if (earlier && !thisBinding) {
      return SIDE_EARLIER;
    }
    return SIDE_OTHER;
  }
}
