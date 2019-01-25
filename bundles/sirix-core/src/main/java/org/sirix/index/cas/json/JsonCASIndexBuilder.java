package org.sirix.index.cas.json;

import org.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.cas.CASIndexBuilder;
import org.sirix.node.immutable.json.ImmutableBooleanNode;
import org.sirix.node.immutable.json.ImmutableNumberNode;
import org.sirix.node.immutable.json.ImmutableStringNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ObjectKeyNode;

/**
 * Builds a content-and-structure (CAS) index.
 *
 * @author Johannes Lichtenberger
 *
 */
final class JsonCASIndexBuilder extends AbstractJsonNodeVisitor {

  private final CASIndexBuilder mIndexBuilderDelegate;

  private final JsonNodeReadOnlyTrx mRtx;

  JsonCASIndexBuilder(final CASIndexBuilder indexBuilderDelegate, final JsonNodeReadOnlyTrx rtx) {
    mIndexBuilderDelegate = indexBuilderDelegate;
    mRtx = rtx;
  }

  @Override
  public VisitResult visit(ImmutableStringNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableBooleanNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableNumberNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  private long getPathClassRecord(ImmutableNode node) {
    mRtx.moveTo(node.getParentKey());
    final long PCR = mRtx.isDocumentRoot()
        ? 0
        : mRtx.isObjectKey()
            ? ((ObjectKeyNode) mRtx.getNode()).getPathNodeKey()
            : mRtx.moveToParent().get().isDocumentRoot()
                ? 0
                : ((ObjectKeyNode) mRtx.getNode()).getPathNodeKey();
    return PCR;
  }

}
