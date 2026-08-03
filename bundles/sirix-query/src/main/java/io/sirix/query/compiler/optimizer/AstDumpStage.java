package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;

/**
 * Prints the AST to stderr when {@code -Dsirix.debug.ast=true}, and is otherwise a no-op.
 *
 * <p>Exists because writing a rewrite stage against a guessed AST shape is a slow way to be wrong:
 * a stage that does not match its pattern silently does nothing, which is indistinguishable from a
 * stage that matched and did not help. Dumping the tree once turns that into a two-minute check.
 */
public final class AstDumpStage implements Stage {

  private static final boolean DUMP = Boolean.getBoolean("sirix.debug.ast");

  private final String label;

  public AstDumpStage(final String label) {
    this.label = label;
  }

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    if (DUMP) {
      final StringBuilder sb = new StringBuilder(256);
      sb.append("=== AST (").append(label).append(") ===\n");
      print(ast, 0, sb);
      System.err.print(sb);
    }
    return ast;
  }

  private static void print(final AST node, final int depth, final StringBuilder sb) {
    if (node == null) {
      return;
    }
    sb.append("  ".repeat(depth)).append(node.getClass().getSimpleName()).append('[')
      .append(node.getType()).append(']');
    final Object value = node.getValue();
    if (value != null) {
      sb.append(" value=").append(value);
    }
    sb.append('\n');
    for (int i = 0; i < node.getChildCount(); i++) {
      print(node.getChild(i), depth + 1, sb);
    }
  }
}
