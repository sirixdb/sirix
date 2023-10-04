package io.sirix.index.cas.xml;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import io.sirix.index.cas.CASIndexBuilder;
import io.sirix.node.immutable.xml.ImmutableAttributeNode;
import io.sirix.node.immutable.xml.ImmutableText;

/**
 * Builds a content-and-structure (CAS) index.
 *
 * @author Johannes Lichtenberger
 *
 */
final class XmlCASIndexBuilder extends AbstractXmlNodeVisitor {

  private final CASIndexBuilder mIndexBuilderDelegate;

  private final XmlNodeReadOnlyTrx mRtx;

  XmlCASIndexBuilder(final CASIndexBuilder indexBuilderDelegate, final XmlNodeReadOnlyTrx rtx) {
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
