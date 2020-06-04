package org.sirix.xquery.compiler.optimizer.walker;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.sirix.access.trx.node.IndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.compiler.XQExt;
import org.sirix.xquery.json.JsonDBStore;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class JsonPathStep extends AbstractJsonPathWalker {

  public JsonPathStep(final StaticContext sctx, final JsonDBStore jsonDBStore) {
    super(sctx, jsonDBStore);
  }

  @Override
  int getPredicateLevel(Path<QNm> pathToFoundNode, Deque<String> predicateSegmentNames) {
    return 0;
  }

  @Override
  protected AST visit(AST astNode) {
    if (astNode.getType() != XQ.DerefExpr) {
      return astNode;
    }

    final AST replacedASTNode = replaceAstIfIndexApplicable(astNode, null);

    if (replacedASTNode != null) {
      return replacedASTNode;
    }

    return astNode;
  }

  @Override
  Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController) {
    return indexController.getIndexes().findPathIndex(pathToFoundNode);
  }

  @Override
  Optional<AST> getPredicatePathStep(AST node, Deque<String> pathNames) {
    return Optional.empty();
  }

  @Override
  AST replaceFoundAST(AST astNode, String databaseName, String resourceName, int revision,
      Map<IndexDef, List<Path<QNm>>> foundIndexDefs, Map<IndexDef, Integer> predicateLevels) {
    final var indexExpr = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
    indexExpr.setProperty("indexType", foundIndexDefs.keySet().iterator().next().getType());
    indexExpr.setProperty("indexDefs", foundIndexDefs);
    indexExpr.setProperty("databaseName", databaseName);
    indexExpr.setProperty("resourceName", resourceName);
    indexExpr.setProperty("revision", revision);

    final var parentASTNode = astNode.getParent();
    parentASTNode.replaceChild(astNode.getChildIndex(), indexExpr);

    return indexExpr;
  }
}
