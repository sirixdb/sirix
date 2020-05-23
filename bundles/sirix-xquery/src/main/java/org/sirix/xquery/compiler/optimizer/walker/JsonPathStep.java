package org.sirix.xquery.compiler.optimizer.walker;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.Walker;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.sirix.access.Databases;
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
  protected AST visit(AST node) {
    if (node.getType() != XQ.DerefExpr) {
      return node;
    }

    if (node.getChild(0).getType() == XQ.DerefExpr || node.getChild(0).getType() == XQ.FunctionCall) {
      final var pathSegmentNames = new ArrayDeque<String>();
      final var pathSegmentName = node.getChild(node.getChildCount() - 1).getStringValue();
      pathSegmentNames.add(pathSegmentName);

      final var newNode = getPathStep(node, pathSegmentNames);

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
            int level = 0;
            while (!pathSegmentNames.isEmpty()) {
              final var pathSegmentNameToCheck = pathSegmentNames.removeLast();

              var pathNode = checkName(pathSummary, pathSegmentNameToCheck, NodeKind.OBJECT_KEY, level + 1);
              if (pathNode.isEmpty()) {
                final var data = findPathNodeBelowArrays(pathSummary, level, pathSegmentNameToCheck);

                pathNode = data.pathNode;
                level = data.level;
              }

              if (pathNode.isEmpty()) {
                final var parentASTNode = node.getParent();
                parentASTNode.replaceChild(node.getChildIndex(), new AST(XQ.SequenceExpr));
              }

              if (pathSegmentNames.isEmpty()) {
                final var foundPathNode = pathNode.get();
                final var pathToFoundNode = foundPathNode.getPath(pathSummary);
                System.out.println(pathToFoundNode);

                final var indexController = revision == -1
                    ? resMgr.getRtxIndexController(resMgr.getMostRecentRevisionNumber())
                    : resMgr.getRtxIndexController(revision);

                final var indexDef = indexController.getIndexes().findPathIndex(pathToFoundNode);

                if (indexDef.isPresent()) {
                  final var parentASTNode = node.getParent();
                  final var indexExpr = new AST(XQExt.IndexExpr);
                  indexExpr.setProperty("indexDef", indexDef.get());
                  parentASTNode.replaceChild(node.getChildIndex(), indexExpr);
                  return indexExpr;
                }
              }
            }
          }
        }
      }
    }

    return node;
  }

  private Data findPathNodeBelowArrays(PathSummaryReader pathSummary, int level, String pathSegmentNameToCheck) {
    Optional<PathNode> pathNode = checkName(pathSummary, "__array__", NodeKind.ARRAY, level + 1);

    if (pathNode.isPresent()) {
      level++;
      pathNode = checkName(pathSummary, pathSegmentNameToCheck, NodeKind.OBJECT_KEY, level + 1);

      if (pathNode.isEmpty()) {
        return findPathNodeBelowArrays(pathSummary, ++level, pathSegmentNameToCheck);
      }

      return new Data(pathNode, ++level);
    }

    return new Data(Optional.empty(), 0);
  }

  private Optional<PathNode> checkName(PathSummaryReader pathSummary, String pathSegmentNameToCheck, NodeKind nodeKind,
      int level) {
    return pathSummary.matchLevel(new QNm(pathSegmentNameToCheck), level, nodeKind);
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

  private static final class Data {
    final Optional<PathNode> pathNode;

    final int level;

    private Data(Optional<PathNode> pathNode, int level) {
      this.pathNode = pathNode;
      this.level = level;
    }
  }
}
