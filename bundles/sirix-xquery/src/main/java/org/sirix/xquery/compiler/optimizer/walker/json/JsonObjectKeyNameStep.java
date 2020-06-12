package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.topdown.ScopeWalker;
import org.sirix.xquery.json.JsonDBStore;

public class JsonObjectKeyNameStep extends ScopeWalker {

  private final JsonDBStore jsonItemStore;

  public JsonObjectKeyNameStep(JsonDBStore jsonItemStore) {
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
