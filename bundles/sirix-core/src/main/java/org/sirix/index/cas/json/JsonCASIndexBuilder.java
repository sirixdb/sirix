package org.sirix.index.cas.json;

import org.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.cas.CASIndexBuilder;
import org.sirix.node.immutable.json.*;
import org.sirix.node.interfaces.immutable.ImmutableNode;

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
  public VisitResult visit(ImmutableObjectStringNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableBooleanNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableObjectBooleanNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableNumberNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableObjectNumberNode node) {
    final long PCR = getPathClassRecord(node);

    return mIndexBuilderDelegate.process(node, PCR);
  }

  private long getPathClassRecord(ImmutableNode node) {
    mRtx.moveTo(node.getParentKey());
    final long PCR = mRtx.isDocumentRoot()
        ? 0
        : mRtx.isObjectKey()
            ? ((ImmutableObjectKeyNode) mRtx.getNode()).getPathNodeKey()
            : mRtx.moveToParent().trx().isDocumentRoot()
                ? 0
                : ((ImmutableObjectKeyNode) mRtx.getNode()).getPathNodeKey();
    return PCR;
  }

}
