package org.sirix.xquery.compiler.optimizer.walker;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.Walker;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.sirix.access.Databases;
import org.sirix.axis.IncludeSelf;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.path.summary.PathNode;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;
import org.sirix.xquery.compiler.XQExt;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public final class JsonPathStep extends Walker {
  /**
   * User home directory.
   */
  private static final String USER_HOME = System.getProperty("user.home");

  /**
   * Storage for databases: Sirix data in home directory.
   */
  private static final Path LOCATION = Paths.get("/tmp", "sirix", "json-path1");//Paths.get(USER_HOME, "sirix-data");

  public JsonPathStep(StaticContext sctx) {
    super(sctx);
  }

  @Override
  protected AST visit(AST astNode) {
    if (astNode.getType() != XQ.DerefExpr) {
      return astNode;
    }

    if (astNode.getChild(0).getType() == XQ.DerefExpr || astNode.getChild(0).getType() == XQ.FunctionCall) {
      final var pathSegmentNames = new ArrayDeque<String>();
      final var pathSegmentName = astNode.getChild(astNode.getChildCount() - 1).getStringValue();
      pathSegmentNames.add(pathSegmentName);

      final var newNode = getPathStep(astNode, pathSegmentNames);

      if (newNode.isPresent()) {
        final var newChildNode = newNode.get();

        if (new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "doc").equals(newChildNode.getValue())) {
          final var databaseName = newChildNode.getChild(0).getStringValue();
          final var resourceName = newChildNode.getChild(1).getStringValue();

          final int revision;
          if (newChildNode.getChildCount() > 2) {
            revision = (int) newChildNode.getChild(2).getValue();
          } else {
            revision = -1;
          }

          final var path = LOCATION.resolve(databaseName);

          try (final var database = Databases.openJsonDatabase(path);
               final var resMgr = database.openResourceManager(resourceName);
               final var pathSummary = revision == -1 ? resMgr.openPathSummary() : resMgr.openPathSummary(revision)) {

            if (!pathSegmentNames.isEmpty()) {
              final var pathSegmentNameToCheck = pathSegmentNames.removeLast();
              var pathNodeKeys = initialPathSummaryNodeCheck(pathSummary, pathSegmentNameToCheck);

              while (!pathNodeKeys.isEmpty() && !pathSegmentNames.isEmpty()) {
                final var finalPathSegmentNameToCheck = pathSegmentNames.removeFirst();

                final var pathNodeKeysToAdd =
                    getSubsequentPathNodeKeys(pathSummary, pathNodeKeys, finalPathSegmentNameToCheck);

                pathNodeKeys = new ArrayList<>();
                pathNodeKeys.addAll(pathNodeKeysToAdd);
              }

              if (pathNodeKeys.isEmpty()) {
                final var parentASTNode = astNode.getParent();
                parentASTNode.replaceChild(astNode.getChildIndex(), new AST(XQ.SequenceExpr));
              }

              if (pathSegmentNames.isEmpty()) {
                boolean notFound = false;
                final var foundIndexDefs = new ArrayList<>();

                for (final int pathNodeKey : pathNodeKeys) {
                  final var foundPathNode = pathSummary.getPathNodeForPathNodeKey(pathNodeKey);
                  final var pathToFoundNode = foundPathNode.getPath(pathSummary);

                  final var indexController = revision == -1
                      ? resMgr.getRtxIndexController(resMgr.getMostRecentRevisionNumber())
                      : resMgr.getRtxIndexController(revision);

                  final var indexDef = indexController.getIndexes().findPathIndex(pathToFoundNode);

                  if (indexDef.isEmpty()) {
                    notFound = true;
                    break;
                  }

                  foundIndexDefs.add(indexDef.get());
                }

                if (!notFound) {
                  final var parentASTNode = astNode.getParent();
                  final var indexExpr = new AST(XQExt.IndexExpr);
                  indexExpr.setProperty("indexDefs", foundIndexDefs);
                  parentASTNode.replaceChild(astNode.getChildIndex(), indexExpr);
                }
              }
            }
          }
        }
      }
    }

    return astNode;
  }

  private List<Integer> getSubsequentPathNodeKeys(PathSummaryReader pathSummary, ArrayList<Integer> pathNodeKeys,
      String finalPathSegmentNameToCheck) {
    final var pathNodeKeysToAdd = new ArrayList<Integer>();

    pathNodeKeys.forEach(pathNodeKey -> {
      final var descendantPathNodeKeys =
          pathSummary.matchDescendants(new QNm(finalPathSegmentNameToCheck), pathNodeKey, IncludeSelf.NO);

      for (int i = descendantPathNodeKeys.nextSetBit(0); i >= 0;
          i = descendantPathNodeKeys.nextSetBit(i + 1)) {
        // operate on index i here
        pathNodeKeysToAdd.add(i);

        if (i == Integer.MAX_VALUE) {
          break; // or (i+1) would overflow
        }
      }
    });
    return pathNodeKeysToAdd;
  }

  private ArrayList<Integer> initialPathSummaryNodeCheck(PathSummaryReader pathSummary, String pathSegmentNameToCheck) {
    final var bitSet = pathSummary.match(new QNm(pathSegmentNameToCheck), 0);
    var pathNodeKeys = new ArrayList<Integer>();

    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      // operate on index i here
      pathNodeKeys.add(i);

      if (i == Integer.MAX_VALUE) {
        break; // or (i+1) would overflow
      }
    }
    return pathNodeKeys;
  }

  private Optional<AST> getPathStep(AST node, Deque<String> pathNames) {
    for (int i = 0, length = node.getChildCount(); i < length; i++) {
      final var step = node.getChild(i);

      if (step.getType() == XQ.FilterExpr) {
        return Optional.empty();
      }

      if (step.getType() == XQ.DerefExpr) {
        final var pathSegmentName = step.getChild(step.getChildCount() - 1).getStringValue();
        pathNames.add(pathSegmentName);
        return getPathStep(step, pathNames);
      }

      if (step.getType() == XQ.FunctionCall) {
        return Optional.of(step);
      }
    }

    return Optional.empty();
  }
}
