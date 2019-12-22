package org.sirix.service.xml.serialize;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.node.immutable.xdm.ImmutableAttributeNode;
import org.sirix.node.immutable.xdm.ImmutableComment;
import org.sirix.node.immutable.xdm.ImmutableDocumentNode;
import org.sirix.node.immutable.xdm.ImmutableElement;
import org.sirix.node.immutable.xdm.ImmutableNamespace;
import org.sirix.node.immutable.xdm.ImmutablePI;
import org.sirix.node.immutable.xdm.ImmutableText;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

public final class XmlMaxLevelVisitor implements XmlNodeVisitor {

  private final long maxLevel;

  private XmlNodeReadOnlyTrx rtx;

  private long currentLevel;

  private VisitResultType lastVisitResultType;

  public XmlMaxLevelVisitor(final long maxLevel) {
    this.maxLevel = maxLevel;
  }

  public XmlMaxLevelVisitor setTrx(final XmlNodeReadOnlyTrx rtx) {
    this.rtx = rtx;
    return this;
  }

  public VisitResultType getLastVisitResultType() {
    return lastVisitResultType;
  }

  public long getCurrentLevel() {
    return currentLevel;
  }

  public long getMaxLevel() {
    return maxLevel;
  }

  private void adaptLevel(ImmutableStructNode node) {
    if (node.hasFirstChild())
      currentLevel++;
    else if (!node.hasRightSibling()) {
      do {
        rtx.moveToParent();
        currentLevel--;
      } while (!rtx.hasRightSibling() && currentLevel > 0);
    }
  }

  private VisitResult getVisitResultType() {
    if (currentLevel >= maxLevel) {
      currentLevel--;
      lastVisitResultType = VisitResultType.SKIPSUBTREE;
      return lastVisitResultType;
    }
    lastVisitResultType = VisitResultType.CONTINUE;
    return lastVisitResultType;
  }

  @Override
  public VisitResult visit(ImmutablePI node) {
    return getVisitResultType();
  }

  @Override
  public VisitResult visit(ImmutableComment node) {
    return getVisitResultType();
  }

  @Override
  public VisitResult visit(ImmutableElement node) {
    adaptLevel(node);
    final var visitResult = getVisitResultType();
    return visitResult;
  }

  @Override
  public VisitResult visit(ImmutableAttributeNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableNamespace node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableText node) {
    return getVisitResultType();
  }

  @Override
  public VisitResult visit(ImmutableDocumentNode node) {
    return VisitResultType.CONTINUE;
  }

}
