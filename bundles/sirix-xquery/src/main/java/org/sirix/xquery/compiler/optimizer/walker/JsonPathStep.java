package org.sirix.xquery.compiler.optimizer.walker;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.Walker;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.sirix.index.IndexDef;
import org.sirix.xquery.compiler.XQExt;
import org.sirix.xquery.json.JsonDBStore;

import java.util.*;

public final class JsonPathStep extends Walker {

  private static final int MIN_NODE_NUMBER = 50_000;

  private final JsonDBStore jsonDBStore;

  public JsonPathStep(final StaticContext sctx, final JsonDBStore jsonDBStore) {
    super(sctx);

    this.jsonDBStore = jsonDBStore;
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

        try (final var jsonCollection = jsonDBStore.lookup(databaseName);
             final var resMgr = jsonCollection.getDatabase().openResourceManager(resourceName);
             final var rtx = revision == -1 ? resMgr.beginNodeReadOnlyTrx() : resMgr.beginNodeReadOnlyTrx(revision);
             final var pathSummary = revision == -1 ? resMgr.openPathSummary() : resMgr.openPathSummary(revision)) {
          if (rtx.getDescendantCount() < MIN_NODE_NUMBER) {
            return astNode;
          }

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
          final var foundIndexDefs = new HashMap<IndexDef, List<org.brackit.xquery.util.path.Path<QNm>>>();

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

            foundIndexDefs.computeIfAbsent(indexDef.get(), (unused) -> new ArrayList<>()).add(pathToFoundNode);
          }

          if (!notFound) {
            final var indexExpr = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
            indexExpr.setProperty("indexDefs", foundIndexDefs);
            indexExpr.setProperty("databaseName", databaseName);
            indexExpr.setProperty("resourceName", resourceName);
            indexExpr.setProperty("revision", revision);

            final var parentASTNode = astNode.getParent();
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
