package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.JoinKeyPreferenceWalker;

/**
 * Optimizer stage that runs {@link JoinKeyPreferenceWalker} between Brackit's predicate reordering
 * and its join recognition: the join is keyed on an equality (a hash join) rather than on whichever
 * general comparison the pulled-up selection chain happens to head, and every single-side predicate
 * of the right input filters the build side before the join. Generic — it applies to every FLWOR
 * with a join-capable where clause — and answers are unchanged, since it only reorders selections.
 *
 * <p>
 * {@code -Dsirix.optimizer.joinKeyPreference=false} disables the stage (read when the optimizer is
 * created).
 * </p>
 */
public final class JoinKeyPreferenceStage implements Stage {

  /** System property that disables the stage. */
  public static final String ENABLED_PROPERTY = "sirix.optimizer.joinKeyPreference";

  private final boolean enabled;

  public JoinKeyPreferenceStage() {
    this.enabled = Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));
  }

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    if (!enabled) {
      return ast;
    }
    return new JoinKeyPreferenceWalker(sctx).walk(ast);
  }
}
