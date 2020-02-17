package org.sirix.index.path.xml;

import org.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.path.PathIndexBuilder;
import org.sirix.node.immutable.xdm.ImmutableAttributeNode;
import org.sirix.node.immutable.xdm.ImmutableElement;

public final class XmlPathIndexBuilder extends AbstractXmlNodeVisitor {

  private final PathIndexBuilder mPathIndexBuilder;

  public XmlPathIndexBuilder(final PathIndexBuilder pathIndexBuilderDelegate) {
    mPathIndexBuilder = pathIndexBuilderDelegate;
  }

  @Override
  public VisitResult visit(ImmutableElement node) {
    return mPathIndexBuilder.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(ImmutableAttributeNode node) {
    return mPathIndexBuilder.process(node, node.getPathNodeKey());
  }

}
