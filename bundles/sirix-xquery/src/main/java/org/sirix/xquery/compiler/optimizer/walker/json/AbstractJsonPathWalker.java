package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.topdown.ScopeWalker;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.trx.node.IndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.index.IndexDef;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;
import org.sirix.xquery.compiler.XQExt;
import org.sirix.xquery.json.JsonDBStore;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;

abstract class AbstractJsonPathWalker extends ScopeWalker {

  private static final int MIN_NODE_NUMBER = Cfg.asInt("org.sirix.xquery.optimize.min.node.number", 0);

  private final JsonDBStore jsonDBStore;

  public AbstractJsonPathWalker(JsonDBStore jsonDBStore) {
    this.jsonDBStore = jsonDBStore;
  }

  protected AST replaceAstIfIndexApplicable(AST astNode, AST predicateNode, Type type) {
    final var firstChildNode = astNode.getChild(0);

    if (!(firstChildNode.getType() == XQ.DerefExpr || firstChildNode.getType() == XQ.ArrayAccess
        || firstChildNode.getType() == XQ.FunctionCall)) {
      return null;
    }

    final var pathData = traversePath(astNode, predicateNode);

    if (pathData == null) {
      return astNode;
    }

    final var node = pathData.node();
    final var pathSegmentNamesToArrayIndexes = pathData.pathSegmentNamesToArrayIndexes();
    final var predicateSegmentNamesToArrayIndexes = pathData.predicatePathSegmentNamesToArrayIndexes();
    final var predicateLeafNode = pathData.predicateLeafNode();

    if (node == null || pathSegmentNamesToArrayIndexes.size() <= 1) {
      return astNode;
    }

    if (!(isDocumentNodeFunction(node) || isIndexExpr(node))) {
      return astNode;
    }

    final RevisionData revisionData = getRevisionData(node);

    try (final var jsonCollection = jsonDBStore.lookup(revisionData.databaseName());
         final var resMgr = jsonCollection.getDatabase().beginResourceSession(revisionData.resourceName());
         final var rtx = revisionData.revision() == -1
             ? resMgr.beginNodeReadOnlyTrx()
             : resMgr.beginNodeReadOnlyTrx(revisionData.revision());
         final var pathSummary = revisionData.revision() == -1
             ? resMgr.openPathSummary()
             : resMgr.openPathSummary(revisionData.revision())) {
      if (rtx.getDescendantCount() < MIN_NODE_NUMBER) {
        return astNode;
      }

      // path node keys of all paths, which have the right most field of the query in its path
      var queryPathSegment = pathSegmentNamesToArrayIndexes.getLast();
      var pathNodeKeys = findFurthestFromRootPathNodes(astNode,
                                                       !queryPathSegment.arrayIndexes().isEmpty()
                                                           ? "__array__"
                                                           : queryPathSegment.pathSegmentName(),
                                                       pathSummary,
                                                       !queryPathSegment.arrayIndexes().isEmpty()
                                                           ? NodeKind.ARRAY
                                                           : NodeKind.OBJECT_KEY);

      var pathNodeKeysToRemove = pathNodeKeys.stream()
                                             .filter(pathNodeKey -> Paths.isPathNodeNotAQueryResult(
                                                 pathSegmentNamesToArrayIndexes,
                                                 pathSummary,
                                                 pathNodeKey))
                                             .toList();

      // remove path node keys which do not belong to the query result
      pathNodeKeys.removeIf(pathNodeKeysToRemove::contains);

      if (pathNodeKeys.isEmpty()) {
        // no path node keys found: replace with empty sequence node
        final var parentASTNode = astNode.getParent();
        final var emptySequence = new AST(XQ.EmptySequenceType);
        parentASTNode.replaceChild(astNode.getChildIndex(), emptySequence);
        return emptySequence;
      }

      final var foundIndexDefsToPaths = new HashMap<IndexDef, List<Path<QNm>>>();
      final var foundIndexDefsToPredicateLevels = new HashMap<IndexDef, Integer>();

      final var predicateSegmentNames = toPredicateSegmentNames(predicateSegmentNamesToArrayIndexes);

      removeFirstPredicateSegmentNameIfPredicateLeafNodeIsContextItemAndParentOfCtxItemIsAnArrayAccessExpr(
          predicateLeafNode,
          predicateSegmentNames);

      boolean notFound = findIndexDefsForPathNodeKeys(predicateNode,
                                                      type,
                                                      predicateSegmentNames,
                                                      revisionData,
                                                      resMgr,
                                                      pathSummary,
                                                      pathNodeKeys,
                                                      foundIndexDefsToPaths,
                                                      foundIndexDefsToPredicateLevels);

      if (!notFound) {
        return replaceFoundAST(astNode,
                               revisionData,
                               foundIndexDefsToPaths,
                               foundIndexDefsToPredicateLevels,
                               pathSegmentNamesToArrayIndexes,
                               predicateLeafNode);
      }
    }

    return null;
  }

  private void removeFirstPredicateSegmentNameIfPredicateLeafNodeIsContextItemAndParentOfCtxItemIsAnArrayAccessExpr(
      AST predicateLeafNode, Deque<String> predicateSegmentNames) {
    if (predicateLeafNode != null && predicateLeafNode.getType() == XQ.ContextItemExpr
        && predicateLeafNode.getParent().getType() == XQ.ArrayAccess && !predicateSegmentNames.isEmpty()) {
      predicateSegmentNames.removeLast();
    }
  }

  private Deque<String> toPredicateSegmentNames(final Deque<QueryPathSegment> predicateSegmentNamesToArrayIndexes) {
    return predicateSegmentNamesToArrayIndexes.stream()
                                              .map(QueryPathSegment::pathSegmentName)
                                              .collect(Collector.of(ArrayDeque::new, ArrayDeque::addFirst, (d1, d2) -> {
                                                d2.addAll(d1);
                                                return d2;
                                              }));
  }

  protected PathData traversePath(final AST node, final AST predicateNode) {
    final var pathSegmentNamesToArrayIndexes = new ArrayDeque<QueryPathSegment>();

    final Optional<AST> predicateLeafNode;

    if (predicateNode != null) {
      final var pathSegmentName = predicateNode.getChild(1).getStringValue();
      pathSegmentNamesToArrayIndexes.push(new QueryPathSegment(pathSegmentName, new ArrayDeque<>()));
      predicateLeafNode = getPredicatePathStep(predicateNode, pathSegmentNamesToArrayIndexes);
      if (predicateLeafNode.isEmpty()) {
        return null;
      }
    } else {
      predicateLeafNode = Optional.empty();
    }

    final var predicatePathSegmentNamesToArrayIndexes = new ArrayDeque<>(pathSegmentNamesToArrayIndexes);

    final Optional<AST> newNode;

    if (node.getType() == XQ.DerefExpr) {
      // otherwise the pathSegmentName has been added in the getPredicatePathStep method already (only if context item _plus_ array index access expr)
      if (!(predicateLeafNode.isPresent() && predicateLeafNode.get().getType() == XQ.ContextItemExpr
          && predicateLeafNode.get().getParent().getType() == XQ.ArrayAccess)) {
        final var pathSegmentName = node.getChild(1).getStringValue();
        pathSegmentNamesToArrayIndexes.push(new QueryPathSegment(pathSegmentName, new ArrayDeque<>()));
      }

      newNode = getPathStep(node, pathSegmentNamesToArrayIndexes);
    } else {
      assert node.getType() == XQ.ArrayAccess;

      if (node.getChildCount() == 2) {
        final var arrayAstNode = processArrayAccess(null, pathSegmentNamesToArrayIndexes, node);

        final Optional<AST> derefNode =
            processLastArrayAccess(pathSegmentNamesToArrayIndexes, node, arrayAstNode, false);

        if (derefNode.isPresent()) {
          final AST currDerefNode = derefNode.get();
          newNode = getPathStep(currDerefNode, pathSegmentNamesToArrayIndexes);
        } else {
          newNode = Optional.of(derefNode.get());
        }
      } else {
        return null;
      }
    }

    return newNode.map(unwrappedNode -> new PathData(pathSegmentNamesToArrayIndexes,
                                                     predicatePathSegmentNamesToArrayIndexes,
                                                     unwrappedNode,
                                                     predicateLeafNode.orElse(null))).orElse(null);
  }

  private List<Integer> findFurthestFromRootPathNodes(AST astNode, String pathSegmentNameToCheck,
      PathSummaryReader pathSummary, NodeKind nodeKind) {
    var pathNodeKeys = new ArrayList<Integer>();
    var pathNodeKeyBitmap = pathSummary.match(new QNm(pathSegmentNameToCheck), 0, nodeKind);

    if (pathNodeKeyBitmap.isEmpty()) {
      final var parentASTNode = astNode.getParent();
      parentASTNode.replaceChild(astNode.getChildIndex(), new AST(XQ.SequenceExpr));
    }

    for (int i = pathNodeKeyBitmap.nextSetBit(0); i >= 0; i = pathNodeKeyBitmap.nextSetBit(i + 1)) {
      // operate on index i here
      pathNodeKeys.add(i);

      if (i == Integer.MAX_VALUE) {
        break; // or (i+1) would overflow
      }
    }

    return pathNodeKeys;
  }

  private boolean findIndexDefsForPathNodeKeys(AST predicateNode, Type type, Deque<String> predicateSegmentNames,
      RevisionData revisionData, JsonResourceSession resMgr, PathSummaryReader pathSummary, List<Integer> pathNodeKeys,
      Map<IndexDef, List<Path<QNm>>> foundIndexDefsToPaths, Map<IndexDef, Integer> foundIndexDefsToPredicateLevels) {
    boolean notFound = false;

    for (final int pathNodeKey : pathNodeKeys) {
      final var foundPathNode = pathSummary.getPathNodeForPathNodeKey(pathNodeKey);
      assert foundPathNode != null;
      final var pathToFoundNode = foundPathNode.getPath(pathSummary);

      final var indexController = revisionData.revision() == -1
          ? resMgr.getRtxIndexController(resMgr.getMostRecentRevisionNumber())
          : resMgr.getRtxIndexController(revisionData.revision());

      final var indexDef = findIndex(pathToFoundNode, indexController, type);

      if (indexDef.isEmpty()) {
        notFound = true;
        break;
      }

      if (predicateNode != null) {
        int predicateLevel = getPredicateLevel(pathToFoundNode, predicateSegmentNames);
        foundIndexDefsToPredicateLevels.put(indexDef.get(), predicateLevel);
      }

      foundIndexDefsToPaths.computeIfAbsent(indexDef.get(), (unused) -> new ArrayList<>()).add(pathToFoundNode);
    }

    return notFound;
  }

  private RevisionData getRevisionData(final AST node) {
    final String databaseName;
    final String resourceName;
    final int revision;

    if (isDocumentNodeFunction(node)) {
      databaseName = node.getChild(0).getStringValue();
      resourceName = node.getChild(1).getStringValue();

      if (node.getChildCount() > 2) {
        revision = (int) node.getChild(2).getValue();
      } else {
        revision = -1;
      }
    } else {
      databaseName = (String) node.getProperty("databaseName");
      resourceName = (String) node.getProperty("resourceName");
      revision = (Integer) node.getProperty("revision");
    }

    return new RevisionData(databaseName, resourceName, revision);
  }

  private boolean isIndexExpr(AST newChildNode) {
    return newChildNode.getType() == XQExt.IndexExpr;
  }

  abstract int getPredicateLevel(Path<QNm> pathToFoundNode, Deque<String> predicateSegmentNames);

  abstract AST replaceFoundAST(AST astNode, RevisionData revisionData, Map<IndexDef, List<Path<QNm>>> foundIndexDefs,
      Map<IndexDef, Integer> predicateLevels, Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes,
      AST predicateLeafNode);

  abstract Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController, Type type);

  private boolean isDocumentNodeFunction(AST newChildNode) {
    return new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "doc").equals(newChildNode.getValue())
        || new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open").equals(newChildNode.getValue());
  }

  protected Optional<AST> getPredicatePathStep(AST node, Deque<QueryPathSegment> predicatePathSegmentNames) {
    return Optional.empty();
  }

  Optional<AST> getPathStep(AST node, Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes) {
    for (int i = 0, length = node.getChildCount(); i < length; i++) {
      final var step = node.getChild(i);

      if (step.getType() == XQ.ArrayAccess) {
        if (step.getChildCount() == 2) {
          final var arrayAstNode = processArrayAccess(null, pathSegmentNamesToArrayIndexes, step);

          return processLastArrayAccess(pathSegmentNamesToArrayIndexes, step, arrayAstNode, true);
        }
      }

      if (step.getType() == XQ.DerefExpr) {
        final var pathSegmentName = step.getChild(step.getChildCount() - 1).getStringValue();

        pathSegmentNamesToArrayIndexes.push(new QueryPathSegment(pathSegmentName, new ArrayDeque<>()));

        return getPathStep(step, pathSegmentNamesToArrayIndexes);
      }

      if (step.getType() == XQ.FunctionCall) {
        return Optional.of(step);
      }

      if (step.getType() == XQ.VariableRef) {
        final Optional<AST> optionalLetBindNode = getScopes().stream().filter(currentScope -> {
          if (currentScope.getType() == XQ.LetBind || currentScope.getType() == XQ.ForBind) {
            final AST varNode = currentScope.getChild(0).getChild(0);
            return step.getValue().equals(varNode.getValue());
          }
          return false;
        }).findFirst();

        return optionalLetBindNode.map(returnFunctionCallOrIndexExprNodeIfPresent(pathSegmentNamesToArrayIndexes));
      }

      if (step.getType() == XQ.ContextItemExpr) {
        return Optional.of(step);
      }
    }

    return Optional.empty();
  }

  protected Optional<AST> processLastArrayAccess(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, AST step,
      AST arrayAstNode, boolean doPathRecursion) {
    final var possiblyDerefAstNode = arrayAstNode.getChild(0);
    final var indexAstNode = arrayAstNode.getChild(1);

    if (indexAstNode.getType() == XQ.SequenceExpr && indexAstNode.getChildCount() == 0) {
      if (possiblyDerefAstNode.getType() == XQ.FunctionCall) {
        adaptPathNamesToArrayIndexesWithNewArrayIndex(null, pathSegmentNamesToArrayIndexes, Integer.MIN_VALUE);

        return Optional.of(possiblyDerefAstNode);
      }

      final String pathSegmentName;

      if (possiblyDerefAstNode.getType() == XQ.DerefExpr) {
        pathSegmentName = possiblyDerefAstNode.getChild(step.getChildCount() - 1).getStringValue();
      } else {
        pathSegmentName = pathSegmentNamesToArrayIndexes.peek().pathSegmentName();
      }

      adaptPathNamesToArrayIndexesWithNewArrayIndex(pathSegmentName, pathSegmentNamesToArrayIndexes, Integer.MIN_VALUE);

      //      if (possiblyDerefAstNode.getType() == XQ.ContextItemExpr) {
      //        return Optional.of(possiblyDerefAstNode);
      //      }

      if (doPathRecursion) {
        return getPathStep(possiblyDerefAstNode, pathSegmentNamesToArrayIndexes);
      } else {
        return Optional.of(possiblyDerefAstNode);
      }
    }

    if (possiblyDerefAstNode.getType() == XQ.FunctionCall) {
      adaptPathNamesToArrayIndexesWithNewArrayIndex(null,
                                                    pathSegmentNamesToArrayIndexes,
                                                    ((Int32) indexAstNode.getValue()).intValue());

      return Optional.of(possiblyDerefAstNode);
    } else if (possiblyDerefAstNode.getType() == XQ.VariableRef) {
      final Optional<AST> optionalLetBindNode = getScopes().stream().filter(currentScope -> {
        if (currentScope.getType() == XQ.LetBind || currentScope.getType() == XQ.ForBind) {
          final AST varNode = currentScope.getChild(0).getChild(0);
          return possiblyDerefAstNode.getValue().equals(varNode.getValue());
        }
        return false;
      }).findFirst();

      return optionalLetBindNode.map(returnFunctionCallOrIndexExprNodeIfPresent(pathSegmentNamesToArrayIndexes));
    } else {
      final var pathSegmentName = possiblyDerefAstNode.getChild(step.getChildCount() - 1).getStringValue();

      adaptPathNamesToArrayIndexesWithNewArrayIndex(pathSegmentName,
                                                    pathSegmentNamesToArrayIndexes,
                                                    ((Int32) indexAstNode.getValue()).intValue());

      if (doPathRecursion) {
        return getPathStep(possiblyDerefAstNode, pathSegmentNamesToArrayIndexes);
      } else {
        return Optional.of(possiblyDerefAstNode);
      }
    }
  }

  private Function<AST, AST> returnFunctionCallOrIndexExprNodeIfPresent(
      Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes) {
    return node -> {
      if (node.getType() == XQ.LetBind) {
        return processLetBind(pathSegmentNamesToArrayIndexes, node);
      } else if (node.getType() == XQ.ForBind) {
        return processForBind(pathSegmentNamesToArrayIndexes, node);
      }

      return null;
    };
  }

  private AST processForBind(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, AST node) {
    final var stepNode = node.getChild(1);

    if (stepNode.getType() == XQ.DerefExpr) {
      final var firstPathSegmentNamesToArrayIndexes = new ArrayDeque<QueryPathSegment>();

      final var pathSegmentName = stepNode.getChild(stepNode.getChildCount() - 1).getStringValue();

      firstPathSegmentNamesToArrayIndexes.push(new QueryPathSegment(pathSegmentName, new ArrayDeque<>()));

      final var astNode = getPathStep(stepNode, firstPathSegmentNamesToArrayIndexes);

      return astNode.map(currAstNode -> {
        var iter = firstPathSegmentNamesToArrayIndexes.descendingIterator();
        while (iter.hasNext()) {
          pathSegmentNamesToArrayIndexes.addFirst(iter.next());
        }
        return currAstNode;
      }).orElse(null);
    } else if (stepNode.getType() == XQExt.IndexExpr) {
      final Deque<QueryPathSegment> currentPathSegmentNamesToArrayIndexes =
          (Deque<QueryPathSegment>) stepNode.getProperty("pathSegmentNamesToArrayIndexes");

      var iter = currentPathSegmentNamesToArrayIndexes.descendingIterator();
      while (iter.hasNext()) {
        pathSegmentNamesToArrayIndexes.addFirst(iter.next());
      }

      return stepNode;
    }

    return null;
  }

  private AST processLetBind(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, AST astNode) {
    final AST varNode = astNode.getChild(1);

    if (varNode.getType() == XQ.FunctionCall) {
      return varNode;
    } else if (varNode.getType() == XQ.DerefExpr) {
      return getPathStep(astNode, pathSegmentNamesToArrayIndexes).orElse(null);
    }

    return null;
  }

  protected AST processArrayAccess(String pathNameForIndexes, Deque<QueryPathSegment> pathNamesToArrayIndexes,
      AST astNode) {
    if (astNode.getType() == XQ.ArrayAccess) {
      var firstChildAstNode = astNode.getChild(0);
      final var indexAstNode = astNode.getChild(1);

      if (firstChildAstNode.getType() == XQ.ArrayAccess) {
        if (pathNameForIndexes == null) {
          var clonedAstNode = firstChildAstNode;
          while (clonedAstNode.getChild(0).getType() == XQ.ArrayAccess) {
            clonedAstNode = clonedAstNode.getChild(0);
          }
          var possiblyDerefAstNode = clonedAstNode.getChild(0);

          if (possiblyDerefAstNode.getType() == XQ.DerefExpr) {
            pathNameForIndexes = possiblyDerefAstNode.getChild(1).getStringValue();
          } else if (possiblyDerefAstNode.getType() == XQ.ContextItemExpr) {
            pathNameForIndexes = getPathNameFromContextItem(possiblyDerefAstNode);

            if (pathNameForIndexes == null) {
              return null;
            }
          }
        }

        if (indexAstNode.getType() == XQ.SequenceExpr && indexAstNode.getChildCount() == 0) {
          adaptPathNamesToArrayIndexesWithNewArrayIndex(pathNameForIndexes, pathNamesToArrayIndexes, Integer.MIN_VALUE);
          return processArrayAccess(pathNameForIndexes, pathNamesToArrayIndexes, firstChildAstNode);
        }

        adaptPathNamesToArrayIndexesWithNewArrayIndex(pathNameForIndexes,
                                                      pathNamesToArrayIndexes,
                                                      ((Int32) indexAstNode.getValue()).intValue());
        return processArrayAccess(pathNameForIndexes, pathNamesToArrayIndexes, firstChildAstNode);
      }
    }
    return astNode;
  }

  protected void adaptPathNamesToArrayIndexesWithNewArrayIndex(String pathNameForIndexes,
      Deque<QueryPathSegment> pathNamesToArrayIndexes, int index) {
    if (pathNamesToArrayIndexes.isEmpty()) {
      pathNamesToArrayIndexes.push(new QueryPathSegment(pathNameForIndexes, new ArrayDeque<>()));
    } else {
      if (!Objects.equals(pathNamesToArrayIndexes.peek().pathSegmentName(), pathNameForIndexes)) {
        pathNamesToArrayIndexes.push(new QueryPathSegment(pathNameForIndexes, new ArrayDeque<>()));
      }
    }

    pathNamesToArrayIndexes.peek().arrayIndexes().add(index);
  }

  protected String getPathNameFromContextItem(AST possiblyDerefAstNode) {
    String pathNameForIndexes = null;
    while (possiblyDerefAstNode.getParent() != null && possiblyDerefAstNode.getParent().getType() != XQ.FilterExpr) {
      possiblyDerefAstNode = possiblyDerefAstNode.getParent();
    }

    if (possiblyDerefAstNode.getParent() != null && possiblyDerefAstNode.getParent().getType() == XQ.FilterExpr) {
      possiblyDerefAstNode = possiblyDerefAstNode.getParent();
      while (possiblyDerefAstNode.getType() != XQ.DerefExpr) {
        possiblyDerefAstNode = possiblyDerefAstNode.getChild(0);
      }
    }

    if (possiblyDerefAstNode.getType() == XQ.DerefExpr) {
      pathNameForIndexes = possiblyDerefAstNode.getChild(1).getStringValue();
    }

    return pathNameForIndexes;
  }
}
