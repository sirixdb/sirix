package org.sirix.indexes;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.AbsStructForwardingNode;
import org.sirix.node.EKind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;

public class PathNode extends AbsStructForwardingNode {

  private final NodeDelegate mNodeDel;
  private final StructNodeDelegate mStructNodeDel;
  private final NameNodeDelegate mNameNodeDel;

  public PathNode(final NodeDelegate pNodeDel,
    final StructNodeDelegate pStructNodeDel, final NameNodeDelegate pNameNodeDel) {
    mNodeDel = checkNotNull(pNodeDel);
    mStructNodeDel = checkNotNull(pStructNodeDel);
    mNameNodeDel = pNameNodeDel;
  }

  @Override
  public EKind getKind() {
    return EKind.PATH;
  }

  @Override
  public EVisitResult acceptVisitor(final IVisitor pVisitor) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return mNodeDel;
  }

}
