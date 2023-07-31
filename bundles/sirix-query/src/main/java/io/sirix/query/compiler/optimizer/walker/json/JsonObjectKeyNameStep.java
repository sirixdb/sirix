package io.sirix.query.compiler.optimizer.walker.json;

import io.sirix.query.compiler.XQExt;
import io.sirix.query.json.JsonDBStore;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JsonObjectKeyNameStep extends AbstractJsonPathWalker {

  public JsonObjectKeyNameStep(JsonDBStore jsonItemStore) {
    super(jsonItemStore);
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

    final AST replaceNode = replaceAstIfIndexApplicable(astNode, null, null);

    if (replaceNode == null) {
      return astNode;
    }

    return replaceNode;
  }

  @Override
  Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController, Type type) {
    return indexController.getIndexes().findNameIndex(pathToFoundNode.tail());
  }

  @Override
  AST replaceFoundAST(AST astNode, RevisionData revisionData, Map<IndexDef, List<Path<QNm>>> foundIndexDefs,
      Map<IndexDef, Integer> predicateLevels, Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, AST predicateLeafNode) {
    final var indexExpr = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
    indexExpr.setProperty("indexType", IndexType.NAME);
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
