package org.sirix.service.json.serialize;

import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.json.*;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

public final class JsonMaxLevelVisitor implements JsonNodeVisitor {

  private final long maxLevel;

  private JsonNodeReadOnlyTrx rtx;

  private long currentLevel;

  private VisitResultType lastVisitResultType;

  private boolean isFirst = true;

  public JsonMaxLevelVisitor(final long maxLevel) {
    this.maxLevel = maxLevel;
  }

  public JsonMaxLevelVisitor setTrx(final JsonNodeReadOnlyTrx rtx) {
    this.rtx = rtx;
    return this;
  }

  public long getCurrentLevel() {
    return currentLevel;
  }

  public long getMaxLevel() {
    return maxLevel;
  }

  private void adaptLevel(ImmutableStructNode node) {
    if (node.hasFirstChild() && !isFirst)
      currentLevel++;
    else if (!node.hasRightSibling()) {
      do {
        if (rtx.getParentKind() != NodeKind.OBJECT_KEY)
          currentLevel--;
        rtx.moveToParent();
      } while (!rtx.hasRightSibling() && currentLevel > 0);
    }
  }

  private VisitResult getVisitResultType() {
    if (currentLevel >= maxLevel) {
      lastVisitResultType = VisitResultType.SKIPSUBTREE;
      return lastVisitResultType;
    }
    lastVisitResultType = VisitResultType.CONTINUE;
    return lastVisitResultType;
  }

  @Override
  public VisitResult visit(ImmutableArrayNode node) {
    adaptLevel(node);
    isFirst = false;
    final var visitResult = getVisitResultType();
    return visitResult;
  }

  @Override
  public VisitResult visit(ImmutableObjectNode node) {
    adaptLevel(node);
    isFirst = false;
    final var visitResult = getVisitResultType();
    return visitResult;
  }

  @Override
  public VisitResult visit(ImmutableObjectKeyNode node) {
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableBooleanNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableStringNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableNumberNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableNullNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectBooleanNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectStringNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectNumberNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectNullNode node) {
    adaptLevel(node);
    isFirst = false;
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableJsonDocumentRootNode node) {
    isFirst = false;
    return VisitResultType.CONTINUE;
  }
}
