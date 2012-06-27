package org.sirix.indexes;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.AbsStructForwardingNode;
import org.sirix.node.ENode;
import org.sirix.node.ElementNode;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;

public class PathSynopsisNode extends AbsStructForwardingNode {

  private final ElementNode mNode;

  public PathSynopsisNode(final ElementNode pNode) {
    mNode = checkNotNull(pNode);
  }
  
  @Override
  public ENode getKind() {
    return ENode.PATH_KIND;
  }

  @Override
  public EVisitResult acceptVisitor(IVisitor pVisitor) {
    return null;
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mNode.getStructNodeDelegate();
  }

  @Override
  protected NodeDelegate delegate() {
    return mNode.getNodeDelegate();
  }

}
