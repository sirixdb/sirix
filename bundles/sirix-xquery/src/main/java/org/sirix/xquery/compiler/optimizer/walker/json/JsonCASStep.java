package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Bits;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.util.path.Path;
import org.sirix.access.trx.node.IndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.compiler.XQExt;
import org.sirix.xquery.json.JsonDBStore;

import java.util.*;

public final class JsonCASStep extends AbstractJsonPathWalker {

  private final ComparatorData comparatorData;

  private Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes;

  public JsonCASStep(final JsonDBStore jsonDBStore) {
    super(jsonDBStore);
    comparatorData = new ComparatorData();
  }

  @Override
  int getPredicateLevel(Path<QNm> pathToFoundNode, Deque<String> predicateSegmentNames) {
    String pathSegment = predicateSegmentNames.removeFirst();
    assert pathToFoundNode != null;
    final var pathSteps = pathToFoundNode.steps();
    int level = 0;

    for (int i = pathSteps.size() - 1; i >= 0; i--) {
      final var step = pathSteps.get(i);

      if (Path.Axis.CHILD_ARRAY.equals(step.getAxis())) {
        level++;
      } else if (step.getAxis() == Path.Axis.CHILD_OBJECT_FIELD && step.getValue().equals(new QNm(pathSegment))) {
        level++;

        if (predicateSegmentNames.isEmpty()) {
          pathSegment = null;
        } else {
          pathSegment = predicateSegmentNames.removeFirst();
        }
      } else if (pathSegment == null) {
//        if (pathSteps.get(i + 1).getAxis() == Path.Axis.CHILD_ARRAY) {
//          level--;
//        }
        break;
      }
    }

    if (level == 0) {
      level++;
    }

    return level;
  }

  @Override
  AST replaceFoundAST(AST astNode, RevisionData revisionData, Map<IndexDef, List<Path<QNm>>> foundIndexDefs,
      Map<IndexDef, Integer> predicateLevels, Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, AST predicateLeafNode) {
//    if (this.pathSegmentNamesToArrayIndexes != null && checkIfDifferentPathsAreCompared(pathSegmentNamesToArrayIndexes)) {
//      return null;
//    }

    final var indexExpr = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
    indexExpr.setProperty("indexType", foundIndexDefs.keySet().iterator().next().getType());
    indexExpr.setProperty("indexDefs", foundIndexDefs);
    indexExpr.setProperty("databaseName", revisionData.databaseName());
    indexExpr.setProperty("resourceName", revisionData.resourceName());
    indexExpr.setProperty("revision", revisionData.revision());
    indexExpr.setProperty("predicateLevel", predicateLevels);
    indexExpr.setProperty("atomic", comparatorData.getAtomic());
    indexExpr.setProperty("comparator", comparatorData.getComparator());
    indexExpr.setProperty("upperBoundAtomic", comparatorData.getUpperBoundAtomic());
    indexExpr.setProperty("upperBoundComparator", comparatorData.getUpperBoundComparator());
    indexExpr.setProperty("pathSegmentNamesToArrayIndexes", pathSegmentNamesToArrayIndexes);
    indexExpr.setProperty("predicateLeafNode", predicateLeafNode);

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

  private boolean checkIfDifferentPathsAreCompared(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes) {
    return !(this.pathSegmentNamesToArrayIndexes.equals(pathSegmentNamesToArrayIndexes));
  }

  @Override
  Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController, Type type) {
    return indexController.getIndexes().findCASIndex(pathToFoundNode, type);
  }

  @Override
  public Optional<AST> getPredicatePathStep(AST node, Deque<QueryPathSegment> predicatePathSegmentsToArrayIndexes) {
    for (int i = 0, length = node.getChildCount(); i < length; i++) {
      final var step = node.getChild(i);

      if (step.getType() == XQ.ArrayAccess) {
        if (step.getChildCount() == 2) {
          final var arrayAstNode = processArrayAccess(null, predicatePathSegmentsToArrayIndexes, step);

          final var derefAstNode = arrayAstNode.getChild(0);
          final var indexAstNode = arrayAstNode.getChild(1);

          if (indexAstNode.getType() == XQ.SequenceExpr && indexAstNode.getChildCount() == 0) {
            String pathSegmentName;
            if (derefAstNode.getType() == XQ.FunctionCall) {
              var indexes = new ArrayDeque<Integer>();
              indexes.add(Integer.MIN_VALUE);
              predicatePathSegmentsToArrayIndexes.push(new QueryPathSegment(null, indexes));

              return Optional.of(derefAstNode);
            } else if (derefAstNode.getType() == XQ.ContextItemExpr) {
              pathSegmentName = getPathNameFromContextItem(derefAstNode);

              adaptPathNamesToArrayIndexesWithNewArrayIndex(pathSegmentName,
                                                            predicatePathSegmentsToArrayIndexes,
                                                            Integer.MIN_VALUE);

              return Optional.of(derefAstNode);
            } else {
              pathSegmentName = derefAstNode.getChild(step.getChildCount() - 1).getStringValue();

              adaptPathNamesToArrayIndexesWithNewArrayIndex(pathSegmentName,
                                                            predicatePathSegmentsToArrayIndexes,
                                                            Integer.MIN_VALUE);

              return getPredicatePathStep(derefAstNode, predicatePathSegmentsToArrayIndexes);
            }
          }

          final var pathSegmentName = derefAstNode.getChild(step.getChildCount() - 1).getStringValue();

          adaptPathNamesToArrayIndexesWithNewArrayIndex(pathSegmentName,
                                                        predicatePathSegmentsToArrayIndexes,
                                                        ((Int32) indexAstNode.getValue()).intValue());

          return getPredicatePathStep(derefAstNode, predicatePathSegmentsToArrayIndexes);
        }
      }

      if (step.getType() == XQ.DerefExpr) {
        final var pathSegmentName = step.getChild(step.getChildCount() - 1).getStringValue();

        predicatePathSegmentsToArrayIndexes.push(new QueryPathSegment(pathSegmentName, new ArrayDeque<>()));

        return getPredicatePathStep(step, predicatePathSegmentsToArrayIndexes);
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
    final var predicateAstNode = astNode.getChild(1);

    if (predicateAstNode.getChildCount() != 1) {
      return astNode;
    }

    final var predicateChildAstNode = predicateAstNode.getChild(0);

    if (predicateChildAstNode.getType() == XQ.AndExpr) {
      processPredicateChildAstNode(astNode, leftChild, predicateChildAstNode.getChild(0), true, false);

      final var comparator = comparatorData.getComparator();
      if (!"ValueCompGT".equals(comparator) && !"GeneralCompGT".equals(comparator) && !"ValueCompGE".equals(comparator)
          && !"GeneralCompGE".equals(comparator)) {
        return null;
      }

      final var node =
          processPredicateChildAstNode(astNode, leftChild, predicateChildAstNode.getChild(1), false, false);

      final var upperBoundComparator = comparatorData.getUpperBoundComparator();
      if (!"ValueCompLT".equals(upperBoundComparator) && !"GeneralCompLT".equals(upperBoundComparator)
          && !"ValueCompLE".equals(upperBoundComparator) && !"GeneralCompLE".equals(upperBoundComparator)) {
        return null;
      }

      return node;

    }

    return processPredicateChildAstNode(astNode, leftChild, predicateChildAstNode, false, true);
  }

  private AST processPredicateChildAstNode(AST astNode, AST leftChild, AST predicateChildAstNode,
      boolean firstInAndComparison, boolean noAndComparison) {
    if (predicateChildAstNode.getChildCount() != 3) {
      return astNode;
    }

    final var comparisonKindChild = predicateChildAstNode.getChild(0);

    final var comparator = comparisonKindChild.getStringValue();

    final var derefPredicateChild = predicateChildAstNode.getChild(1);
    final var typeKindChild = predicateChildAstNode.getChild(2);

    if (!(typeKindChild.getValue() instanceof final Atomic atomic)) {
      return astNode;
    }

    final var atomicType = atomic.type();

    if (firstInAndComparison || noAndComparison) {
      comparatorData.setAtomic(atomic);
      comparatorData.setComparator(comparator);
    } else {
      comparatorData.setUpperBoundAtomic(atomic);
      comparatorData.setUpperBoundComparator(comparator);
    }

    if (derefPredicateChild.getType() != XQ.DerefExpr) {
      return astNode;
    }

    if (leftChild.getType() == XQ.DerefExpr) {
      if (noAndComparison || !firstInAndComparison) {
        return getAst(astNode, derefPredicateChild, leftChild, atomicType);
      } else {
        return processFirstInAndComparison(leftChild, derefPredicateChild);
      }
    } else if (leftChild.getType() == XQ.ArrayAccess) {
      if (leftChild.getChild(0).getType() != XQ.DerefExpr || leftChild.getChild(1).getType() != XQ.SequenceExpr) {
        return astNode;
      }

      final var derefNode = leftChild.getChild(0);
      if (noAndComparison || !firstInAndComparison) {
        return getAst(astNode, derefPredicateChild, derefNode, atomicType);
      } else {
        return processFirstInAndComparison(derefNode, derefPredicateChild);
      }
    } else if (leftChild.getType() == XQ.FunctionCall) {
      if (!new QNm(Bits.BIT_NSURI, Bits.BIT_PREFIX, "array-values").equals(astNode.getChild(0).getValue())) {
        return astNode;
      }

      if (leftChild.getChild(0).getType() != XQ.DerefExpr) {
        return astNode;
      }

      final var derefNode = leftChild.getChild(0);
      if (noAndComparison || !firstInAndComparison) {
        return getAst(astNode, derefPredicateChild, derefNode, atomicType);
      } else {
        return processFirstInAndComparison(derefNode, derefPredicateChild);
      }
    }

    return astNode;
  }

  private AST processFirstInAndComparison(AST astNode, AST derefPredicateChild) {
    if (!(astNode.getChild(0).getType() == XQ.DerefExpr || astNode.getChild(0).getType() == XQ.ArrayAccess
        || astNode.getChild(0).getType() == XQ.FunctionCall)) {
      return null;
    }

    final var pathData = traversePath(astNode, derefPredicateChild);

    final var node = pathData.node();

    if (node == null) {
      return astNode;
    }

    pathSegmentNamesToArrayIndexes = pathData.pathSegmentNamesToArrayIndexes();

    return node;
  }

  private AST getAst(AST astNode, AST predicateChild, AST derefNode, Type type) {
    final var node = replaceAstIfIndexApplicable(derefNode, predicateChild, type);

    if (node != null) {
      return node;
    }

    return astNode;
  }
}
