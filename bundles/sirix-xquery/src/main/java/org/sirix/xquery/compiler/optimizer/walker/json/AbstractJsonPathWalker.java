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
import org.sirix.api.json.JsonResourceManager;
import org.sirix.index.IndexDef;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;
import org.sirix.xquery.compiler.XQExt;
import org.sirix.xquery.json.JsonDBStore;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract class AbstractJsonPathWalker extends ScopeWalker {

  private static final int MIN_NODE_NUMBER = Cfg.asInt("org.sirix.xquery.optimize.min.node.number", 0);

  private final JsonDBStore jsonDBStore;

  public AbstractJsonPathWalker(JsonDBStore jsonDBStore) {
    this.jsonDBStore = jsonDBStore;
  }

  protected AST replaceAstIfIndexApplicable(AST astNode, AST predicateNode, Type type) {
    boolean foundDerefAncestor = findDerefAncestor(astNode);

    if (foundDerefAncestor || !(astNode.getChild(0).getType() == XQ.DerefExpr
        || astNode.getChild(0).getType() == XQ.ArrayAccess || astNode.getChild(0).getType() == XQ.FunctionCall)) {
      return null;
    }

    final var pathData = traversePath(astNode, predicateNode);

    final var node = pathData.node();
    final var pathSegmentNames = pathData.pathSegmentNames();
    final var arrayIndexes = pathData.arrayIndexes();
    final var predicateSegmentNames = pathData.predicateNames();

    if (node == null || pathSegmentNames.size() <= 1) {
      return astNode;
    }

    if (!(isDocumentNodeFunction(node) || isIndexExpr(node))) {
      return astNode;
    }

    final RevisionData revisionData = getRevisionData(node);

    try (final var jsonCollection = jsonDBStore.lookup(revisionData.databaseName());
         final var resMgr = jsonCollection.getDatabase().openResourceManager(revisionData.resourceName());
         final var rtx = revisionData.revision() == -1
             ? resMgr.beginNodeReadOnlyTrx()
             : resMgr.beginNodeReadOnlyTrx(revisionData.revision());
         final var pathSummary = revisionData.revision() == -1
             ? resMgr.openPathSummary()
             : resMgr.openPathSummary(revisionData.revision())) {
      if (rtx.getDescendantCount() < MIN_NODE_NUMBER) {
        return astNode;
      }

      final var pathSegmentNameToCheck = pathSegmentNames.removeFirst();
      var pathNodeKeys = findFurthestFromRootPathNodes(astNode, pathSegmentNameToCheck, pathSummary);

      // re-add path segment
      pathSegmentNames.addFirst(pathSegmentNameToCheck);

      var pathNodeKeysToRemove = pathNodeKeys.stream()
                                             .filter(pathNodeKey -> pathNodeKeyToRemove(pathSegmentNames,
                                                                                     pathSummary,
                                                                                     pathNodeKey))
                                             .collect(Collectors.toList());

      // remove path node keys which do not belong to the query result
      pathNodeKeys.removeIf(pathNodeKeysToRemove::contains);

      if (pathNodeKeys.isEmpty()) {
        // no path node keys found: replace with empty sequence node
        final var parentASTNode = astNode.getParent();
        final var emptySequence = new AST(XQ.SequenceExpr);
        parentASTNode.replaceChild(astNode.getChildIndex(), emptySequence);
        return emptySequence;
      }

      final var foundIndexDefsToPaths = new HashMap<IndexDef, List<Path<QNm>>>();
      final var foundIndexDefsToPredicateLevels = new HashMap<IndexDef, Integer>();

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
                               arrayIndexes,
                               pathSegmentNames);
      }
    }

    return null;
  }

  protected PathData traversePath(final AST node, final AST predicateNode) {
    final var pathSegmentNames = new ArrayDeque<String>();
    final var arrayIndexes = new HashMap<String, Deque<Integer>>();

    if (predicateNode != null) {
      final var pathSegmentName = predicateNode.getChild(1).getStringValue();
      pathSegmentNames.add(pathSegmentName);
      final var predicateLeafNode = getPredicatePathStep(predicateNode, pathSegmentNames, arrayIndexes);
      if (predicateLeafNode.isEmpty()) {
        return null;
      }
    }

    final var predicateSegmentNames = new ArrayDeque<>(pathSegmentNames);

    final var pathSegmentName = node.getChild(1).getStringValue();
    pathSegmentNames.add(pathSegmentName);
    final var newNode = getPathStep(node, pathSegmentNames, arrayIndexes);

    return new PathData(pathSegmentNames, arrayIndexes, predicateSegmentNames, newNode.get());
  }

  private List<Integer> findFurthestFromRootPathNodes(AST astNode, String pathSegmentNameToCheck,
      PathSummaryReader pathSummary) {
    var pathNodeKeys = new ArrayList<Integer>();
    var pathNodeKeyBitmap = pathSummary.match(new QNm(pathSegmentNameToCheck), 0, NodeKind.OBJECT_KEY);

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
      RevisionData revisionData, JsonResourceManager resMgr, PathSummaryReader pathSummary, List<Integer> pathNodeKeys,
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

  private boolean pathNodeKeyToRemove(Deque<String> pathSegmentNames,
      PathSummaryReader pathSummary, int pathNodeKey) {
    final var currentPathSegmentNames = new ArrayDeque<>(pathSegmentNames);
    pathSummary.moveTo(pathNodeKey);
    var candidatePath = pathSummary.getPath();
    String pathSegment = currentPathSegmentNames.removeFirst();
    assert candidatePath != null;
    final var pathSteps = candidatePath.steps();
    boolean found = false;

    for (int i = pathSteps.size() - 1; i >= 0; i--) {
      final var step = pathSteps.get(i);

      if (found && currentPathSegmentNames.isEmpty() && !Path.Axis.CHILD_ARRAY.equals(step.getAxis())) {
        break;
      } else if (step.getAxis() == Path.Axis.CHILD && step.getValue().equals(new QNm(pathSegment))) {
        found = true;

        if (!currentPathSegmentNames.isEmpty()) {
          pathSegment = currentPathSegmentNames.removeFirst();
        }
      }
    }

    if (!currentPathSegmentNames.isEmpty() || !found) {
      return true;
    }

    return false;
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
      Map<IndexDef, Integer> predicateLevel, Map<String, Deque<Integer>> arrayIndexes, Deque<String> pathSegmentNames);

  abstract Optional<IndexDef> findIndex(Path<QNm> pathToFoundNode,
      IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController, Type type);

  private boolean isDocumentNodeFunction(AST newChildNode) {
    return new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "doc").equals(newChildNode.getValue())
        || new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open").equals(newChildNode.getValue());
  }

  protected boolean findDerefAncestor(AST astNode) {
    boolean foundDerefAncestor = false;
    AST ancestor = astNode.getParent();
    while (ancestor != null && !foundDerefAncestor) {
      foundDerefAncestor = ancestor.getType() == XQ.DerefExpr;
      ancestor = ancestor.getParent();
    }
    return foundDerefAncestor;
  }

  abstract Optional<AST> getPredicatePathStep(AST node, Deque<String> pathNames,
      Map<String, Deque<Integer>> arrayIndexes);

  Optional<AST> getPathStep(AST node, Deque<String> pathNames, Map<String, Deque<Integer>> arrayIndexes) {
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

          return getPathStep(derefAstNode, pathNames, arrayIndexes);
        }
      }

      if (step.getType() == XQ.DerefExpr) {
        final var pathSegmentName = step.getChild(step.getChildCount() - 1).getStringValue();
        pathNames.add(pathSegmentName);
        return getPathStep(step, pathNames, arrayIndexes);
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

        return optionalLetBindNode.map(returnFunctionCallOrIndexExprNodeIfPresent(pathNames, arrayIndexes));
      }
    }

    return Optional.empty();
  }

  private Function<AST, AST> returnFunctionCallOrIndexExprNodeIfPresent(Deque<String> pathNames,
      Map<String, Deque<Integer>> arrayIndexes) {
    return node -> {
      if (node.getType() == XQ.LetBind) {
        return processLetBind(pathNames, arrayIndexes, node);
      } else if (node.getType() == XQ.ForBind) {
        return processForBind(pathNames, arrayIndexes, node);
      }

      return null;
    };
  }

  private AST processForBind(Deque<String> pathNames, Map<String, Deque<Integer>> arrayIndexes, AST node) {
    final var stepNode = node.getChild(1);

    if (stepNode.getType() == XQ.DerefExpr) {
      final var firstPathNames = new ArrayDeque<String>();
      final var firstArrayIndexes = new HashMap<String, Deque<Integer>>();

      final var pathSegmentName = stepNode.getChild(stepNode.getChildCount() - 1).getStringValue();
      firstPathNames.add(pathSegmentName);

      final var astNode = getPathStep(stepNode, firstPathNames, firstArrayIndexes);

      return astNode.map(currAstNode -> {
        pathNames.addAll(firstPathNames);
        arrayIndexes.putAll(firstArrayIndexes);
        return currAstNode;
      }).orElse(null);
    } else if (stepNode.getType() == XQExt.IndexExpr) {
      final Deque<String> indexPathSegmentNames = (Deque<String>) stepNode.getProperty("pathSegmentNames");
      final Map<String, Deque<Integer>> indexArrayIndexes =
          (Map<String, Deque<Integer>>) stepNode.getProperty("arrayIndexes");

      pathNames.addAll(indexPathSegmentNames);
      arrayIndexes.putAll(indexArrayIndexes);
      return stepNode;
    }

    return null;
  }

  private AST processLetBind(Deque<String> pathSegmentNames, Map<String, Deque<Integer>> arrayIndexes, AST astNode) {
    final AST varNode = astNode.getChild(1);

    if (varNode.getType() == XQ.FunctionCall) {
      return varNode;
    } else if (varNode.getType() == XQ.DerefExpr) {
      return getPathStep(astNode, pathSegmentNames, arrayIndexes).orElse(null);
    }

    return null;
  }

  protected AST processArrayAccess(String pathNameForIndexes, Map<String, Deque<Integer>> arrayIndexes, AST astNode) {
    if (astNode.getType() == XQ.ArrayAccess) {
      var firstChildAstNode = astNode.getChild(0);
      final var indexAstNode = astNode.getChild(1);

      if (firstChildAstNode.getType() == XQ.ArrayAccess) {
        if (pathNameForIndexes == null) {
          var clonedAstNode = firstChildAstNode.copyTree();
          while (clonedAstNode.getChild(0).getType() == XQ.ArrayAccess) {
            clonedAstNode = clonedAstNode.getChild(0);
          }
          final var derefAstNode = clonedAstNode.getChild(0);
          pathNameForIndexes = derefAstNode.getChild(1).getStringValue();
        }

        arrayIndexes.computeIfAbsent(pathNameForIndexes, (unused) -> new ArrayDeque<>())
                    .add(((Int32) indexAstNode.getValue()).intValue());
        return processArrayAccess(pathNameForIndexes, arrayIndexes, firstChildAstNode);
      }
    }
    return astNode;
  }
}
