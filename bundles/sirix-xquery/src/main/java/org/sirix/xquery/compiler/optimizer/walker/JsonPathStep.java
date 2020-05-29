package org.sirix.xquery.compiler.optimizer.walker;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.Walker;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.sirix.access.Databases;
import org.sirix.xquery.compiler.XQExt;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Optional;

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

    boolean foundDerefAncestor = findDerefAncestor(astNode);

    if (!foundDerefAncestor && (astNode.getChild(0).getType() == XQ.DerefExpr
        || astNode.getChild(0).getType() == XQ.FunctionCall)) {
      final var pathSegmentNames = new ArrayDeque<String>();
      final var pathSegmentName = astNode.getChild(astNode.getChildCount() - 1).getStringValue();
      pathSegmentNames.add(pathSegmentName);

      final var newNode = getPathStep(astNode, pathSegmentNames);

      if (newNode.isEmpty() || pathSegmentNames.size() <= 1) {
        return astNode;
      }

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
          var pathNodeKeysToRemove = new ArrayList<Integer>();
          var pathNodeKeys = new ArrayList<Integer>();

          final var pathSegmentNameToCheck = pathSegmentNames.removeFirst();

          var pathNodeKeyBitmap = pathSummary.match(new QNm(pathSegmentNameToCheck), 0);

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

          final QNm arrayName = new QNm("__array__");

          for (final var pathNodeKey : pathNodeKeys) {
            final var currentPathSegmentNames = new ArrayDeque<>(pathSegmentNames);
            pathSummary.moveTo(pathNodeKey);
            var candidatePath = pathSummary.getPath();
            String pathSegment = currentPathSegmentNames.removeFirst();
            final var pathSteps = candidatePath.steps();
            boolean found = false;

            for (int i = pathSteps.size() - 1; i >= 0; i--) {
              final var step = pathSteps.get(i);

              if (found && currentPathSegmentNames.isEmpty() && !arrayName.equals(step.getValue())) {
                break;
              } else if (step.getValue().equals(new QNm(pathSegment))) {
                found = true;

                if (!currentPathSegmentNames.isEmpty()) {
                  pathSegment = currentPathSegmentNames.removeFirst();
                }
              }
            }

            if (!currentPathSegmentNames.isEmpty() || !found) {
              pathNodeKeysToRemove.add(pathNodeKey);
            }
          }

          pathNodeKeys.removeIf(pathNodeKeysToRemove::contains);

          if (pathNodeKeys.isEmpty()) {
            final var parentASTNode = astNode.getParent();
            parentASTNode.replaceChild(astNode.getChildIndex(), new AST(XQ.SequenceExpr));
          }

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
            final var indexExpr = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
            indexExpr.setProperty("indexDefs", foundIndexDefs);
            parentASTNode.replaceChild(astNode.getChildIndex(), indexExpr);
            return indexExpr;
          }
        }
      }
    }

    return astNode;
  }

  private boolean findDerefAncestor(AST astNode) {
    boolean foundDerefAncestor = false;
    AST ancestor = astNode.getParent();
    while (ancestor != null && !foundDerefAncestor) {
      foundDerefAncestor = ancestor.getType() == XQ.DerefExpr;
      ancestor = ancestor.getParent();
    }
    return foundDerefAncestor;
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
