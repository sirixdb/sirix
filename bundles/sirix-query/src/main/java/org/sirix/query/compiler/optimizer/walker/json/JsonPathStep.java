package org.sirix.query.compiler.optimizer.walker.json;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.util.path.Path;
import org.sirix.access.trx.node.IndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.index.IndexDef;
import org.sirix.query.compiler.XQExt;
import org.sirix.query.json.JsonDBStore;

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
      Map<IndexDef, Integer> predicateLevels, Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, AST predicateLeafNode) {
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
