package org.sirix.xquery.compiler.optimizer.walker;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Bits;
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

public final class JsonCASStep extends AbstractJsonPathWalker {

  private String comparator;

  private Atomic atomic;

  public JsonCASStep(final StaticContext sctx, final JsonDBStore jsonDBStore) {
    super(sctx, jsonDBStore);
  }

  @Override
  int getPredicateLevel(Path<QNm> pathToFoundNode, Deque<String> predicateSegmentNames) {
    String pathSegment = predicateSegmentNames.removeFirst();
    assert pathToFoundNode != null;
    final var pathSteps = pathToFoundNode.steps();
    int level = 0;

    for (int i = pathSteps.size() - 1; i >= 0 && pathSegment != null; i--) {
      final var step = pathSteps.get(i);

      if (Path.Axis.CHILD_ARRAY.equals(step.getAxis())) {
        level++;
      } else if (step.getAxis() == Path.Axis.CHILD && step.getValue().equals(new QNm(pathSegment))) {
        level++;

        if (predicateSegmentNames.isEmpty()) {
          pathSegment = null;
        } else {
          pathSegment = predicateSegmentNames.removeFirst();
        }
      }
    }

    if (level == 0) {
      level++;
    }

    return level;
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
    indexExpr.setProperty("predicateLevel", predicateLevels);
    indexExpr.setProperty("atomic", atomic);
    indexExpr.setProperty("comparator", comparator);

    final var parent = astNode.getParent();
    indexExpr.setProperty("hasBitArrayValuesFunction",
                          parent.getType() == XQ.FunctionCall && new QNm(Bits.BIT_NSURI,
                                                                         Bits.BIT_PREFIX,
                                                                         "array-values").equals(parent.getValue()));

    if (parent.getType() == XQ.FilterExpr) {
      parent.getParent().replaceChild(parent.getChildIndex(), indexExpr);
    } else {
      final var filterExpr = parent.getParent();
      filterExpr.getParent().replaceChild(filterExpr.getChildIndex(), indexExpr);
    }


    return indexExpr;
  }

  @Override
  Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController) {
    return indexController.getIndexes().findCASIndex(pathToFoundNode, atomic.type());
  }

  @Override
  Optional<AST> getPredicatePathStep(AST node, Deque<String> pathNames) {
    for (int i = 0, length = node.getChildCount(); i < length; i++) {
      final var step = node.getChild(i);

      if (step.getType() == XQ.DerefExpr) {
        final var pathSegmentName = step.getChild(step.getChildCount() - 1).getStringValue();
        pathNames.add(pathSegmentName);
        return getPredicatePathStep(step, pathNames);
      }

      if (step.getType() == XQ.ContextItemExpr) {
        return Optional.of(step);
      }
    }

    return Optional.empty();
  }

  @Override
  protected AST visit(AST astNode) {
    if (astNode.getType() != XQ.FilterExpr) {
      return astNode;
    }

    if (astNode.getChildCount() != 2 || (astNode.getChild(0).getType() != XQ.ArrayAccess
        && astNode.getChild(0).getType() != XQ.DerefExpr && astNode.getChild(0).getType() != XQ.FunctionCall)
        || astNode.getChild(1).getType() != XQ.Predicate) {
      return astNode;
    }

    final var leftChild = astNode.getChild(0);
    final var predicateChild = astNode.getChild(1);

    if (predicateChild.getChildCount() != 1) {
      return astNode;
    }

    final var comparisonPredicateChild = predicateChild.getChild(0);

    if (comparisonPredicateChild.getChildCount() != 3) {
      return astNode;
    }

    final var comparisonKindChild = comparisonPredicateChild.getChild(0);
    comparator = comparisonKindChild.getStringValue();
    final var derefPredicateChild = comparisonPredicateChild.getChild(1);
    final var typeKindChild = comparisonPredicateChild.getChild(2);
    atomic = (Atomic) typeKindChild.getValue();

    if (derefPredicateChild.getType() != XQ.DerefExpr) {
      return astNode;
    }

    if (leftChild.getType() == XQ.DerefExpr) {
      return getAst(astNode, derefPredicateChild, leftChild);
    } else if (leftChild.getType() == XQ.ArrayAccess) {
      if (leftChild.getChild(0).getType() != XQ.DerefExpr || leftChild.getChild(1).getType() != XQ.SequenceExpr) {
        return astNode;
      }

      final var derefNode = leftChild.getChild(0);
      return getAst(astNode, derefPredicateChild, derefNode);
    } else if (leftChild.getType() == XQ.FunctionCall) {
      if (!new QNm(Bits.BIT_NSURI, Bits.BIT_PREFIX, "array-values").equals(astNode.getChild(0).getValue())) {
        return astNode;
      }

      if (leftChild.getChild(0).getType() != XQ.DerefExpr) {
        return astNode;
      }

      final var derefNode = leftChild.getChild(0);
      return getAst(astNode, derefPredicateChild, derefNode);
    }

    return astNode;
  }

  private AST getAst(AST astNode, AST predicateChild, AST derefNode) {
    final var node = replaceAstIfIndexApplicable(derefNode, predicateChild);

    if (node != null) {
      return node;
    }

    return astNode;
  }
}
