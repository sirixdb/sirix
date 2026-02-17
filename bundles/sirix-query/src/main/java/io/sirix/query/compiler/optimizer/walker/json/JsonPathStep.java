package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.json.JsonDBStore;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class JsonPathStep extends AbstractJsonPathWalker {

  public JsonPathStep(final JsonDBStore jsonDBStore) {
    super(jsonDBStore);
  }

  @Override
  int getPredicateLevel(Path<QNm> pathToFoundNode, Deque<String> predicateSegmentNames) {
    return 0;
  }

  @Override
  protected AST visit(AST astNode) {
    if (astNode.getType() != XQ.DerefExpr && astNode.getType() != XQ.ArrayAccess) {
      return astNode;
    }

    final AST replaceNode = replaceAstIfIndexApplicable(astNode, null, null);

    if (replaceNode == null) {
      return astNode;
    }

    return replaceNode;
  }

  @Override
  Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController, Type type) {
    return indexController.getIndexes().findPathIndex(pathToFoundNode);
  }

  @Override
  AST replaceFoundAST(AST astNode, RevisionData revisionData, Map<IndexDef, List<Path<QNm>>> foundIndexDefs,
      Map<IndexDef, Integer> predicateLevels, Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes,
      AST predicateLeafNode) {
    final var indexExpr = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
    indexExpr.setProperty("indexType", foundIndexDefs.keySet().iterator().next().getType());
    indexExpr.setProperty("indexDefs", foundIndexDefs);
    indexExpr.setProperty("databaseName", revisionData.databaseName());
    indexExpr.setProperty("resourceName", revisionData.resourceName());
    indexExpr.setProperty("revision", revisionData.revision());
    indexExpr.setProperty("pathSegmentNamesToArrayIndexes", pathSegmentNamesToArrayIndexes);

    final var parentASTNode = astNode.getParent();
    parentASTNode.replaceChild(astNode.getChildIndex(), indexExpr);

    return indexExpr;
  }
}
