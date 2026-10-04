package io.sirix.index.cas.xml;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import io.sirix.index.IndexBuildFinalizer;
import io.sirix.index.IndexType;
import io.sirix.index.cas.CASIndexBuilder;
import io.sirix.node.immutable.xml.ImmutableAttributeNode;
import io.sirix.node.immutable.xml.ImmutableText;
import io.sirix.node.immutable.xml.ImmutableComment;
import io.sirix.node.immutable.xml.ImmutablePI;
import io.sirix.node.interfaces.immutable.ImmutableNameNode;
import io.sirix.settings.Fixed;

/**
 * Builds a content-and-structure (CAS) index.
 *
 * @author Johannes Lichtenberger
 *
 */
final class XmlCASIndexBuilder extends AbstractXmlNodeVisitor implements IndexBuildFinalizer {

  private final CASIndexBuilder mIndexBuilderDelegate;

  private final XmlNodeReadOnlyTrx mRtx;

  XmlCASIndexBuilder(final CASIndexBuilder indexBuilderDelegate, final XmlNodeReadOnlyTrx rtx) {
    mIndexBuilderDelegate = indexBuilderDelegate;
    mRtx = rtx;
  }

  @Override
  public VisitResult visit(ImmutableText node) {
    return mIndexBuilderDelegate.process(node, parentPathNodeKey(node.getParentKey()));
  }

  @Override
  public VisitResult visit(ImmutableAttributeNode node) {
    return mIndexBuilderDelegate.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(final ImmutablePI node) {
    return mIndexBuilderDelegate.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(final ImmutableComment node) {
    return mIndexBuilderDelegate.process(node, parentPathNodeKey(node.getParentKey()));
  }

  private long parentPathNodeKey(final long parentKey) {
    if (parentKey == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      return 0;
    }
    // Read the parent directly: moving the shared cursor changes the node delivered to subsequent
    // builders and can cause the wrapper axis to emit the parent's attributes again.
    final ImmutableNameNode parent = mRtx.getStorageEngineReader().getRecord(parentKey, IndexType.DOCUMENT, -1);
    return parent.getPathNodeKey();
  }

  @Override
  public void finishIndexBuild() {
    mIndexBuilderDelegate.finish();
  }

}
