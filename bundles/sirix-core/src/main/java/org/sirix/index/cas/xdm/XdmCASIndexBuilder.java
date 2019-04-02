package org.sirix.index.cas.xdm;

import org.sirix.access.trx.node.xml.AbstractXdmNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.cas.CASIndexBuilder;
import org.sirix.node.immutable.xdm.ImmutableAttributeNode;
import org.sirix.node.immutable.xdm.ImmutableText;

/**
 * Builds a content-and-structure (CAS) index.
 *
 * @author Johannes Lichtenberger
 *
 */
final class XdmCASIndexBuilder extends AbstractXdmNodeVisitor {

  private final CASIndexBuilder mIndexBuilderDelegate;

  private final XmlNodeReadOnlyTrx mRtx;

  XdmCASIndexBuilder(final CASIndexBuilder indexBuilderDelegate, final XmlNodeReadOnlyTrx rtx) {
    mIndexBuilderDelegate = indexBuilderDelegate;
    mRtx = rtx;
  }

  @Override
  public VisitResult visit(ImmutableText node) {
    mRtx.moveTo(node.getParentKey());
    final long PCR = mRtx.isDocumentRoot()
        ? 0
        : mRtx.getNameNode().getPathNodeKey();

    return mIndexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableAttributeNode node) {
    final long PCR = mRtx.isDocumentRoot()
        ? 0
        : mRtx.getNameNode().getPathNodeKey();

    return mIndexBuilderDelegate.process(node, PCR);
  }

}
