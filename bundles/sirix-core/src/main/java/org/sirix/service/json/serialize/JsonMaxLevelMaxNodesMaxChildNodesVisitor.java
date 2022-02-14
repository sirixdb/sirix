package org.sirix.service.json.serialize;

import org.jetbrains.annotations.NotNull;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.axis.IncludeSelf;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.json.ImmutableArrayNode;
import org.sirix.node.immutable.json.ImmutableBooleanNode;
import org.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import org.sirix.node.immutable.json.ImmutableNullNode;
import org.sirix.node.immutable.json.ImmutableNumberNode;
import org.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.immutable.json.ImmutableObjectNode;
import org.sirix.node.immutable.json.ImmutableObjectNullNode;
import org.sirix.node.immutable.json.ImmutableObjectNumberNode;
import org.sirix.node.immutable.json.ImmutableObjectStringNode;
import org.sirix.node.immutable.json.ImmutableStringNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

import java.util.ArrayDeque;
import java.util.Deque;

public final class JsonMaxLevelMaxNodesMaxChildNodesVisitor implements JsonNodeVisitor {

  private final Deque<Long> rightSiblingNodeKeyStack;

  private final long startNodeKey;

  private final long maxLevel;

  private final long maxNodes;

  private final IncludeSelf includeSelf;

  private final long maxChildNodes;

  private long currentChildNodes = 1;

  private long numberOfVisitedNodesPlusOne = 1;

  private JsonNodeReadOnlyTrx rtx;

  private long currentLevel = 1;

  private VisitResultType lastVisitResultType;

  private boolean isFirst = true;

  private boolean deweyIDsAreStored;

  private long startNodeLevel;

  private final Deque<Long> currentChildNodesPerLevel = new ArrayDeque<>();

  private long lastVisitedNodeKey;

  public JsonMaxLevelMaxNodesMaxChildNodesVisitor(final long startNodeKey, final IncludeSelf includeSelf,
      final long maxLevel, final long maxNodes, final long maxChildNodes) {
    this.startNodeKey = startNodeKey;
    this.includeSelf = includeSelf;
    this.maxLevel = maxLevel;
    this.maxNodes = maxNodes;
    this.maxChildNodes = maxChildNodes;
    rightSiblingNodeKeyStack = new ArrayDeque<>();
  }

  public JsonMaxLevelMaxNodesMaxChildNodesVisitor setTrx(final JsonNodeReadOnlyTrx rtx) {
    this.rtx = rtx;
    deweyIDsAreStored = rtx.getResourceManager().getResourceConfig().areDeweyIDsStored;
    if (deweyIDsAreStored) {
      final var nodeKey = rtx.getNodeKey();
      rtx.moveTo(startNodeKey);
      startNodeLevel = rtx.getDeweyID().getLevel();
      rtx.moveTo(nodeKey);
      if (rtx.isObjectKey()) {
        numberOfVisitedNodesPlusOne = 2;
      }
    }
    return this;
  }

  public long getNumberOfVisitedNodesPlusOne() {
    return numberOfVisitedNodesPlusOne;
  }

  public long getCurrentLevel() {
    return currentLevel;
  }

  public long getCurrentChildNodes() {
    return currentChildNodes;
  }

  public long getMaxNodes() {
    return maxNodes;
  }

  public long getMaxLevel() {
    return maxLevel;
  }

  public long getMaxChildNodes() {
    return maxChildNodes;
  }

  public VisitResultType getLastVisitResultType() {
    return lastVisitResultType;
  }

  private VisitResult getVisitResultType(ImmutableStructNode node) {
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    if (currentLevel > maxLevel) {
      lastVisitResultType = VisitResultType.SKIPSUBTREE;
      return lastVisitResultType;
    }
    if (hasToSkipSiblingNodes()) {
      return adaptCurrentChildNodesAndLevelAndReturnSkipSiblingsResultType(node);
    }
    lastVisitResultType = VisitResultType.CONTINUE;
    lastVisitedNodeKey = rtx.getNodeKey();
    return lastVisitResultType;
  }

  private void adaptLevel(ImmutableStructNode node) {
    if (isFirst) {
      return;
    }
    if (node.getNodeKey() != startNodeKey && node.getNodeKey() == lastVisitedNodeKey) {
      return;
    }
    if (node.hasFirstChild() && currentChildNodes <= maxChildNodes) {
      currentLevel++;

      if (node.hasRightSibling()) {
        currentChildNodesPerLevel.push(currentChildNodes);
      }

      if (deweyIDsAreStored && node.hasRightSibling() && currentChildNodes + 1 <= maxChildNodes) {
        rightSiblingNodeKeyStack.push(node.getRightSiblingKey());
      }

      currentChildNodes = 1;
    } else if (!node.hasRightSibling()) {
      adaptChildNodesAndLevel(node);
    } else {
      currentChildNodes++;
    }
  }

  private void adaptChildNodesAndLevel(ImmutableStructNode node) {
    if (currentChildNodes <= maxChildNodes) {
      adaptCurrentChildNodes();
    }

    ancestorLevel(node);
  }

  private void ancestorLevel(ImmutableStructNode node) {
    if (deweyIDsAreStored) {
      if (rightSiblingNodeKeyStack.isEmpty()) {
        currentLevel = 1;
      } else {
        final long nextNodeKey = rightSiblingNodeKeyStack.pop();
        rtx.moveTo(nextNodeKey);
        currentLevel = startNodeLevel == 0 ? rtx.getDeweyID().getLevel() : rtx.getDeweyID().getLevel() - startNodeLevel + 1;
        rtx.moveTo(node.getNodeKey());
      }
    } else {
      do {
        if (rtx.getParentKind() != NodeKind.OBJECT_KEY) {
          currentLevel--;
        }
        rtx.moveToParent();
      } while (!rtx.hasRightSibling() && currentLevel > 1);
    }
  }

  private boolean hasToTerminateTraversal() {
    return numberOfVisitedNodesPlusOne > maxNodes && rtx.getKind() != NodeKind.OBJECT_KEY;
  }

  @Override
  public VisitResult visit(ImmutableArrayNode node) {
    adaptLevel(node);
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    isFirst = false;
    return getVisitResultType(node);
  }

  @Override
  public VisitResult visit(ImmutableObjectNode node) {
    adaptLevel(node);
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    isFirst = false;
    return getVisitResultType(node);
  }

  private boolean hasToAddNumberOfVisitedNodes(long nodeKey) {
    return !isFirst && (lastVisitedNodeKey != nodeKey || startNodeKey == nodeKey);
  }

  @Override
  public VisitResult visit(ImmutableObjectKeyNode node) {
    if (deweyIDsAreStored && node.hasRightSibling() && currentChildNodes + 1 <= maxChildNodes) {
      rightSiblingNodeKeyStack.push(node.getRightSiblingKey());
    }
    adaptCurrentChildNodesForObjectKeyNodes(node);
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    isFirst = false;
    return determineAndGetVisitResultType(node);
  }

  private void adaptCurrentChildNodesForObjectKeyNodes(final ImmutableObjectKeyNode node) {
    if (node.hasFirstChild()) {
      if (node.hasRightSibling() && currentChildNodes <= maxChildNodes) {
        currentChildNodesPerLevel.push(currentChildNodes);
      }
    } else if (node.hasRightSibling()) {
      currentChildNodes++;
    } else {
      adaptCurrentChildNodes();
    }
  }

  @Override
  public VisitResult visit(ImmutableBooleanNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  private void adaptCurrentChildNodes() {
    if (currentChildNodesPerLevel.isEmpty()) {
      currentChildNodes = maxChildNodes + 1;
    } else {
      currentChildNodes = currentChildNodesPerLevel.pop() + 1;
    }
  }

  @Override
  public VisitResult visit(ImmutableStringNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  @Override
  public VisitResult visit(ImmutableNumberNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  @Override
  public VisitResult visit(ImmutableNullNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  private void incrementNumberOfVisitedNodesIfRequired(long nodeKey) {
    if (hasToAddNumberOfVisitedNodes(nodeKey)) {
      numberOfVisitedNodesPlusOne++;
    }
  }

  @Override
  public VisitResult visit(ImmutableObjectBooleanNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  private boolean hasToSkipSiblingNodes() {
    return currentChildNodes > maxChildNodes;
  }

  @Override
  public VisitResult visit(ImmutableObjectStringNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  @Override
  public VisitResult visit(ImmutableObjectNumberNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  @Override
  public VisitResult visit(ImmutableObjectNullNode node) {
    adaptLevel(node);
    isFirst = false;
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    return determineAndGetVisitResultType(node);
  }

  private VisitResultType determineAndGetVisitResultType(ImmutableStructNode node) {
    if (hasToTerminateTraversal()) {
      lastVisitResultType = VisitResultType.TERMINATE;
      return lastVisitResultType;
    }
    if (hasToSkipSiblingNodes()) {
      return adaptCurrentChildNodesAndLevelAndReturnSkipSiblingsResultType(node);
    }
    lastVisitedNodeKey = node.getNodeKey();
    return VisitResultType.CONTINUE;
  }

  @NotNull
  private VisitResultType adaptCurrentChildNodesAndLevelAndReturnSkipSiblingsResultType(ImmutableStructNode node) {
    final var nodeKey = rtx.getNodeKey();
    if (!(rtx.getParentKind() == NodeKind.OBJECT_KEY && rtx.moveToParent().trx().hasRightSibling())) {
      adaptCurrentChildNodes();
      ancestorLevel(node);
    }
    rtx.moveTo(nodeKey);
    lastVisitResultType = VisitResultType.SKIPSIBLINGS;
    return lastVisitResultType;
  }

  @Override
  public VisitResult visit(ImmutableJsonDocumentRootNode node) {
    incrementNumberOfVisitedNodesIfRequired(node.getNodeKey());
    isFirst = false;
    return VisitResultType.CONTINUE;
  }
}
