package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.JoinKeyPreferenceWalker;

/**
 * Optimizer stage that runs {@link JoinKeyPreferenceWalker} between Brackit's predicate reordering
 * and its join recognition: when the selection chain pulled up below a binding holds an eligible
 * equality, the join is keyed on it (a hash join) rather than on whichever general comparison the
 * chain happens to head, and every single-side predicate of the right input filters the build side
 * before the join. Generic — it applies to every {@code for}- and {@code let}-bound chain with a
 * join-capable where clause — while a chain whose key is already a qualifying equality, and one
 * with no eligible equality at all, keep the key Brackit gives them. Reordering selections yields
 * the same result set, but not the same dynamic errors: a conjunct pushed to the build side is
 * evaluated on rows that never join, so it can raise there, and one that moves behind the join sees
 * only joined pairs. See {@link JoinKeyPreferenceWalker} for which equalities qualify.
 *
 * <p>
 * {@code -Dsirix.optimizer.joinKeyPreference=false} disables the rule: {@link SirixOptimizer} then
 * installs neither this stage nor the anchor requirement it places on Brackit's pipeline (read when
 * the optimizer is created).
 * </p>
 */
public final class JoinKeyPreferenceStage implements Stage {

  /** System property that disables the stage; it is on unless this is set to {@code false}. */
  public static final String ENABLED_PROPERTY = "sirix.optimizer.joinKeyPreference";

  /** Whether the rule is switched on. The sole gate: a disabled rule is never installed at all. */
  public static boolean enabled() {
    return !"false".equalsIgnoreCase(System.getProperty(ENABLED_PROPERTY, "true").trim());
  }

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    return new JoinKeyPreferenceWalker(sctx).walk(ast);
  }
}
