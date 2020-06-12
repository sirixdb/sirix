package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Bits;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.trx.node.IndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.compiler.XQExt;
import org.sirix.xquery.json.JsonDBStore;

import java.util.*;
import java.util.stream.Collectors;

public final class JsonCASStep extends AbstractJsonPathWalker {

  private String comparator;

  private Atomic atomic;

  private String upperBoundComparator;

  private Atomic upperBoundAtomic;

  private Map<String, Deque<Integer>> arrayIndexes;

  private Deque<String> pathSegmentNames;

  private boolean firstInAndComparison = true;

  private boolean noAndComparison;

  public JsonCASStep(final JsonDBStore jsonDBStore) {
    super(jsonDBStore);
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
      } else if (step.getAxis() == Path.Axis.CHILD && step.getValue().equals(new QNm(pathSegment))) {
        level++;

        if (predicateSegmentNames.isEmpty()) {
          pathSegment = null;
        } else {
          pathSegment = predicateSegmentNames.removeFirst();
        }
      } else if (pathSegment == null) {
        if (pathSteps.get(i + 1).getAxis() == Path.Axis.CHILD_ARRAY) {
          level--;
        }
        break;
      }
    }

    if (level == 0) {
      level++;
    }

    return level;
  }

  @Override
  AST replaceFoundAST(AST astNode, String databaseName, String resourceName, int revision,
      Map<IndexDef, List<Path<QNm>>> foundIndexDefs, Map<IndexDef, Integer> predicateLevels,
      Map<String, Deque<Integer>> arrayIndexes, Deque<String> pathSegmentNames) {
    if (!noAndComparison) {
      if (firstInAndComparison) {
        firstInAndComparison = false;
        this.arrayIndexes = arrayIndexes;
        this.pathSegmentNames = pathSegmentNames;
        return null;
      } else {
        if (this.arrayIndexes != null && this.pathSegmentNames != null && checkIfDifferentPathsAreCompared(arrayIndexes,
                                                                                                           pathSegmentNames)) {
          return null;
        }
      }
    }

    final var indexExpr = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
    indexExpr.setProperty("indexType", foundIndexDefs.keySet().iterator().next().getType());
    indexExpr.setProperty("indexDefs", foundIndexDefs);
    indexExpr.setProperty("databaseName", databaseName);
    indexExpr.setProperty("resourceName", resourceName);
    indexExpr.setProperty("revision", revision);
    indexExpr.setProperty("predicateLevel", predicateLevels);
    indexExpr.setProperty("atomic", atomic);
    indexExpr.setProperty("comparator", comparator);

    if (!noAndComparison) {
      indexExpr.setProperty("upperBoundAtomic", upperBoundAtomic);
      indexExpr.setProperty("upperBoundComparator", upperBoundComparator);
    }

    indexExpr.setProperty("arrayIndexes", arrayIndexes);
    indexExpr.setProperty("pathSegmentNames", pathSegmentNames);

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

  private boolean checkIfDifferentPathsAreCompared(Map<String, Deque<Integer>> arrayIndexes,
      Deque<String> pathSegmentNames) {
    if (!(this.arrayIndexes.keySet().equals(arrayIndexes.keySet()))) {
      return true;
    }

    if (!(toList(this.arrayIndexes).equals(toList(arrayIndexes)))) {
      return true;
    }

    if (!(new ArrayList<>(this.pathSegmentNames).equals(new ArrayList<>(pathSegmentNames)))) {
      return true;
    }
    return false;
  }

  private List<Integer> toList(Map<String, Deque<Integer>> arrayIndexes) {
    return arrayIndexes.values().stream().flatMap(indices -> indices.stream()).collect(Collectors.toList());
  }

  @Override
  Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController, Type type) {
    return indexController.getIndexes().findCASIndex(pathToFoundNode, type);
  }

  @Override
  Optional<AST> getPredicatePathStep(AST node, Deque<String> pathNames, Map<String, Deque<Integer>> arrayIndexes) {
    for (int i = 0, length = node.getChildCount(); i < length; i++) {
      final var step = node.getChild(i);

      if (step.getType() == XQ.ArrayAccess) {
        if (step.getChildCount() == 2) {
          final var arrayAstNode = processArrayAccess(null, arrayIndexes, step);

          final var derefAstNode = arrayAstNode.getChild(0);
          final var indexAstNode = arrayAstNode.getChild(1);

          final var pathSegmentName = derefAstNode.getChild(step.getChildCount() - 1).getStringValue();
          pathNames.add(pathSegmentName);

          arrayIndexes.computeIfAbsent(pathSegmentName, (unused) -> new ArrayDeque<>())
                      .add(((Int32) indexAstNode.getValue()).intValue());

          return getPredicatePathStep(derefAstNode, pathNames, arrayIndexes);
        }
      }

      if (step.getType() == XQ.DerefExpr) {
        final var pathSegmentName = step.getChild(step.getChildCount() - 1).getStringValue();
        pathNames.add(pathSegmentName);
        return getPredicatePathStep(step, pathNames, arrayIndexes);
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
      processComparisonAstNode(astNode, leftChild, predicateChildAstNode.getChild(0));

      if (firstInAndComparison) {
        return null;
      }

      if (!"ValueCompGT".equals(comparator) && !"GeneralCompGT".equals(comparator) && !"ValueCompGE".equals(comparator)
          && !"GeneralCompGE".equals(comparator)) {
        return null;
      }

      final var node = processComparisonAstNode(astNode, leftChild, predicateChildAstNode.getChild(1));

      if (!"ValueCompLT".equals(upperBoundComparator) && !"GeneralCompLT".equals(upperBoundComparator) && !"ValueCompLE"
          .equals(upperBoundComparator) && !"GeneralCompLE".equals(upperBoundComparator)) {
        return null;
      }

      return node;

    }

    // no and-comparison
    noAndComparison = true;
    return processComparisonAstNode(astNode, leftChild, predicateChildAstNode);
  }

  private AST processComparisonAstNode(AST astNode, AST leftChild, AST predicateChildAstNode) {
    if (predicateChildAstNode.getChildCount() != 3) {
      return astNode;
    }

    final var comparisonKindChild = predicateChildAstNode.getChild(0);

    if (firstInAndComparison || noAndComparison) {
      comparator = comparisonKindChild.getStringValue();
    } else {
      upperBoundComparator = comparisonKindChild.getStringValue();
    }

    final var derefPredicateChild = predicateChildAstNode.getChild(1);
    final var typeKindChild = predicateChildAstNode.getChild(2);

    if (!(typeKindChild.getValue() instanceof Atomic)) {
      return astNode;
    }

    final Type atomicType;

    if (firstInAndComparison || noAndComparison) {
      atomic = (Atomic) typeKindChild.getValue();
      atomicType = atomic.type();
    } else {
      upperBoundAtomic = (Atomic) typeKindChild.getValue();
      atomicType = upperBoundAtomic.type();
    }

    if (derefPredicateChild.getType() != XQ.DerefExpr) {
      return astNode;
    }

    if (leftChild.getType() == XQ.DerefExpr) {
      return getAst(astNode, derefPredicateChild, leftChild, atomicType);
    } else if (leftChild.getType() == XQ.ArrayAccess) {
      if (leftChild.getChild(0).getType() != XQ.DerefExpr || leftChild.getChild(1).getType() != XQ.SequenceExpr) {
        return astNode;
      }

      final var derefNode = leftChild.getChild(0);
      return getAst(astNode, derefPredicateChild, derefNode, atomicType);
    } else if (leftChild.getType() == XQ.FunctionCall) {
      if (!new QNm(Bits.BIT_NSURI, Bits.BIT_PREFIX, "array-values").equals(astNode.getChild(0).getValue())) {
        return astNode;
      }

      if (leftChild.getChild(0).getType() != XQ.DerefExpr) {
        return astNode;
      }

      final var derefNode = leftChild.getChild(0);
      return getAst(astNode, derefPredicateChild, derefNode, atomicType);
    }

    return astNode;
  }

  private AST getAst(AST astNode, AST predicateChild, AST derefNode, Type type) {
    final var node = replaceAstIfIndexApplicable(derefNode, predicateChild, type);

    if (node != null) {
      return node;
    }

    return astNode;
  }
}
