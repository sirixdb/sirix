package io.sirix.query.compiler.optimizer.walker;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.brackit.query.module.StaticContext;

public final class PathStep extends Walker {
  public PathStep(StaticContext sctx) {
    super(sctx);
  }

  @Override
  protected AST visit(AST node) {
    if (node.getType() != XQ.PathExpr) {
      return node;
    }

    for (int i = 1; i < node.getChildCount(); i++) {
      AST step = node.getChild(i);
      boolean childStep = ((step.getType() == XQ.StepExpr) && (getAxis(step) == XQ.CHILD));
      boolean hasPredicate = (step.getChildCount() > 2);
    }

    return node;
  }

  private int getAxis(AST stepExpr) {
    return stepExpr.getChild(0).getChild(0).getType();
  }
}
