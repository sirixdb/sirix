package org.sirix.service.json.serialize;

import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.axis.IncludeSelf;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.json.*;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

import java.util.ArrayDeque;
import java.util.Deque;

public final class JsonMaxLevelMaxNodesVisitor implements JsonNodeVisitor {

  private final Deque<Long> rightSiblingNodeKeyStack;

  private final long startNodeKey;

  private final long maxLevel;

  private final long maxNodes;

  private final IncludeSelf includeSelf;

  private long numberOfVisitedNodesPlusOne = 1;

  private JsonNodeReadOnlyTrx rtx;

  private long currentLevel = 1;

  private VisitResultType lastVisitResultType;

  private boolean isFirst = true;

  private boolean deweyIDsAreStored;

  private long startNodeLevel;

  public JsonMaxLevelMaxNodesVisitor(final long startNodeKey, final IncludeSelf includeSelf, final long maxLevel,
      final long maxNodes) {
    this.startNodeKey = startNodeKey;
    this.includeSelf = includeSelf;
    this.maxLevel = maxLevel;
    this.maxNodes = maxNodes;
    rightSiblingNodeKeyStack = new ArrayDeque<>();
  }

  public JsonMaxLevelMaxNodesVisitor setTrx(final JsonNodeReadOnlyTrx rtx) {
    this.rtx = rtx;
    deweyIDsAreStored = rtx.getResourceManager().getResourceConfig().areDeweyIDsStored;
    if (deweyIDsAreStored) {
      final var nodeKey = rtx.getNodeKey();
      rtx.moveTo(startNodeKey);
      startNodeLevel = rtx.getDeweyID().getLevel();
      rtx.moveTo(nodeKey);
    }
    return this;
  }

  public long getCurrentLevel() {
    return currentLevel;
  }

  public long getMaxLevel() {
    return maxLevel;
  }

  public VisitResultType getLastVisitResultType() {
    return lastVisitResultType;
  }

  private void adaptLevel(ImmutableStructNode node) {
    if (node.hasFirstChild() && !isFirst) {
      currentLevel++;

      if (deweyIDsAreStored && node.hasRightSibling()) {
        rightSiblingNodeKeyStack.push(node.getRightSiblingKey());
      }
    } else if (!node.hasRightSibling()) {
      if (deweyIDsAreStored) {
        if (rightSiblingNodeKeyStack.isEmpty()) {
          currentLevel = 1;
        } else {
          final long nextNodeKey = rightSiblingNodeKeyStack.pop();
          rtx.moveTo(nextNodeKey);
          currentLevel = rtx.getDeweyID().getLevel() - startNodeLevel;
          rtx.moveTo(node.getNodeKey());
        }
      } else {
        do {
          if (rtx.getParentKind() != NodeKind.OBJECT_KEY)
            currentLevel--;
          rtx.moveToParent();
        } while (!rtx.hasRightSibling() && currentLevel > 1);
      }
    }
  }

  private VisitResult getVisitResultType() {
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    if (currentLevel > maxLevel) {
      lastVisitResultType = VisitResultType.SKIPSUBTREE;
      return lastVisitResultType;
    }
    lastVisitResultType = VisitResultType.CONTINUE;
    return lastVisitResultType;
  }

  private boolean hasToTerminateTraversal() {
    return numberOfVisitedNodesPlusOne > maxNodes && rtx.getKind() != NodeKind.OBJECT_KEY;
  }

  @Override
  public VisitResult visit(ImmutableArrayNode node) {
    adaptLevel(node);
    if (!(node.getNodeKey() == startNodeKey && includeSelf == IncludeSelf.YES && isFirst)) {
      numberOfVisitedNodesPlusOne++;
    }
    isFirst = false;
    return getVisitResultType();
  }

  @Override
  public VisitResult visit(ImmutableObjectNode node) {
    adaptLevel(node);
    if (!(node.getNodeKey() == startNodeKey && includeSelf == IncludeSelf.YES && isFirst)) {
      numberOfVisitedNodesPlusOne++;
    }
    isFirst = false;
    return getVisitResultType();
  }

  @Override
  public VisitResult visit(ImmutableObjectKeyNode node) {
    if (deweyIDsAreStored && node.hasRightSibling()) {
      rightSiblingNodeKeyStack.push(node.getRightSiblingKey());
    }
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableBooleanNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableStringNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableNumberNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableNullNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectBooleanNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectStringNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectNumberNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectNullNode node) {
    adaptLevel(node);
    isFirst = false;
    numberOfVisitedNodesPlusOne++;
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableJsonDocumentRootNode node) {
    if (!(node.getNodeKey() == startNodeKey && includeSelf == IncludeSelf.YES && isFirst)) {
      numberOfVisitedNodesPlusOne++;
    }
    isFirst = false;
    return VisitResultType.CONTINUE;
  }
}
