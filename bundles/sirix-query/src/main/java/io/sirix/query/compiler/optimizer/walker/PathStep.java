package io.sirix.query.compiler.optimizer.walker;

import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.Walker;
import org.brackit.xquery.module.StaticContext;

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
