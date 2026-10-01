package io.sirix.query.compiler.optimizer.walker;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.topdown.ScopeWalker;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Orders the selection chain below a {@code for}/{@code let} binding so that Brackit's join
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
 * <li>the selections that reference only this binding (or nothing bound in the pipeline) — Brackit
 * copies them into the join's right input, so they filter the build side before the join;</li>
 * <li>the equality nearest the head of the chain whose two sides reference this binding and only
 * earlier bindings, respectively — the join key. That is the first eligible equality
 * {@code JoinRewriter} itself reaches, so whenever its own choice is already such an equality the
 * key is left exactly as it was: the rule only ever re-keys away from a non-equality, never from
 * one equality to another;</li>
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

  private int rewrites;

  public JoinKeyPreferenceWalker(final StaticContext sctx) {
    super(sctx);
  }

  /** Number of selection chains this walk reordered. */
  public int rewrites() {
    return rewrites;
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
    rewrites++;
    snapshot();
    refreshScopes(node, true);
    return node;
  }

  /**
   * {@link #EQUALITY} mirrors {@code JoinRewriter}'s eligibility for an equality keyed on this
   * binding: a comparison whose one side references only this binding and whose other side references
   * only earlier bindings, both sides non-static. {@link #RIGHT_ONLY}: every pipeline reference is to
   * this binding (or there is none). Everything else is {@link #RESIDUAL}.
   */
  private int classify(final AST predicate, final Scope bind) {
    final VarRef refs = findVarRefs(predicate);
    if (refs == null) {
      return RIGHT_ONLY;
    }
    final Scope[] scopes = sortScopes(refs);
    boolean usesOther = false;
    for (final Scope scope : scopes) {
      if (scope != bind) {
        usesOther = true;
        break;
      }
    }
    if (!usesOther) {
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
   * Which bindings one comparison side references: only this one, only earlier ones, or anything
   * else.
   */
  private int side(final AST expression, final Scope bind) {
    final VarRef refs = findVarRefs(expression);
    if (refs == null) {
      return SIDE_OTHER; // static side: JoinRewriter does not join on it
    }
    boolean thisBinding = false;
    boolean earlier = false;
    for (final Scope scope : sortScopes(refs)) {
      if (scope == bind) {
        thisBinding = true;
      } else if (scope.compareTo(bind) < 0) {
        earlier = true;
      } else {
        return SIDE_OTHER;
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
