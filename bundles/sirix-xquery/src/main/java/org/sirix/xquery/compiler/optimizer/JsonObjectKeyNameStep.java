package org.sirix.xquery.compiler.optimizer;

import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.topdown.ScopeWalker;
import org.brackit.xquery.module.StaticContext;
import org.sirix.xquery.json.JsonDBStore;

public class JsonObjectKeyNameStep extends ScopeWalker {
  private final StaticContext sctx;
  private final JsonDBStore jsonItemStore;

  public JsonObjectKeyNameStep(StaticContext sctx, JsonDBStore jsonItemStore) {
    this.sctx = sctx;
    this.jsonItemStore = jsonItemStore;
  }

  @Override
  protected AST visit(AST astNode) {
    if (astNode.getType() != XQ.DerefExpr) {
      return astNode;
    }



    return astNode;
  }
}
