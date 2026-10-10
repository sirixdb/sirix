package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.jspecify.annotations.Nullable;

public record RevisionData(String databaseName, String resourceName, int revision, @Nullable AST operand,
    boolean byInstant) {
  public RevisionData(final String databaseName, final String resourceName, final int revision) {
    this(databaseName, resourceName, revision, null, false);
  }

  /** Only repeatable operands may be evaluated again when a historical index is unavailable. */
  static boolean isStableOperand(final AST node) {
    if (node.getType() == XQ.VariableRef || node.getType() == XQ.ContextItemExpr) {
      return true;
    }
    if (node.getChildCount() == 0 && node.getValue() instanceof Atomic && node.getType() != XQ.FunctionCall) {
      return true;
    }
    if (node.getType() == XQ.FunctionCall) {
      if (!(node.getValue() instanceof QNm name)
          || !"http://www.w3.org/2001/XMLSchema".equals(name.getNamespaceURI())) {
        return false;
      }
    } else if (node.getType() != XQ.DerefExpr && node.getType() != XQ.SequenceExpr) {
      return false;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (!isStableOperand(node.getChild(i))) {
        return false;
      }
    }
    return true;
  }

  void bind(final AST index, final AST fallback) {
    index.setProperty("revisionByInstant", byInstant);
    index.addChild(operand == null
        ? new AST(XQ.Int, new Int32(revision))
        : operand.copyTree());
    index.addChild(fallback.copyTree());
  }
}
