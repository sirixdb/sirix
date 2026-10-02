package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.JoinKeyPreferenceWalker;

/**
 * Optimizer stage that runs {@link JoinKeyPreferenceWalker} between Brackit's predicate reordering
 * and its join recognition: when the selection chain pulled up below a {@code for} binding holds an
 * eligible equality, the join is keyed on it (a hash join) rather than on whichever general
 * comparison the chain happens to head, and every single-side predicate of the right input filters
 * the build side before the join. It applies to every {@code for}-bound join with a join-capable
 * where clause; a chain without such an equality, and every {@code let}-bound chain, keeps the plan
 * Brackit gives it. Answers are unchanged, since it only reorders selections.
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
    return Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));
  }

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    return new JoinKeyPreferenceWalker(sctx).walk(ast);
  }
}
