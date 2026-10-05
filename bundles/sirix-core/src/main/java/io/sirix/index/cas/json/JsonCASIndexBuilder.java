package io.sirix.index.cas.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.IndexBuildFinalizer;
import io.sirix.index.cas.CASIndexBuilder;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;

/**
 * Builds a content-and-structure (CAS) index.
 *
 * @author Johannes Lichtenberger
 */
final class JsonCASIndexBuilder extends AbstractJsonNodeVisitor implements IndexBuildFinalizer {

  private final CASIndexBuilder indexBuilderDelegate;

  private final JsonNodeReadOnlyTrx rtx;

  JsonCASIndexBuilder(final CASIndexBuilder indexBuilderDelegate, final JsonNodeReadOnlyTrx rtx) {
    this.indexBuilderDelegate = indexBuilderDelegate;
    this.rtx = rtx;
  }

  @Override
  public VisitResult visit(ImmutableStringNode node) {
    final long PCR = getPathClassRecord(node);

    return indexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableBooleanNode node) {
    final long PCR = getPathClassRecord(node);

    return indexBuilderDelegate.process(node, PCR);
  }

  @Override
  public VisitResult visit(ImmutableNumberNode node) {
    final long PCR = getPathClassRecord(node);

    return indexBuilderDelegate.process(node, PCR);
  }

  // Fused OBJECT_NAMED_* — the pathNodeKey lives ON the fused node itself (not on the
  // parent) because the fused record plays the OBJECT_KEY structural role. CAS index
  // needs (nodeKey, pathNodeKey, value).

  @Override
  public VisitResult visit(final ObjectNamedStringNode node) {
    return indexBuilderDelegate.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(final ObjectNamedNumberNode node) {
    return indexBuilderDelegate.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(final ObjectNamedBooleanNode node) {
    return indexBuilderDelegate.process(node, node.getPathNodeKey());
  }

  @Override
  public void finishIndexBuild() {
    indexBuilderDelegate.finish();
  }

  private long getPathClassRecord(final ImmutableNode node) {
    final long nodeKey = node.getNodeKey();
    try {
      if (!rtx.moveTo(node.getParentKey())) {
        throw new IllegalStateException("CAS value has no parent: " + nodeKey);
      }
      // All builders share this cursor. Restore it before processing the value or dispatching
      // another visitor; otherwise a second CAS builder receives the parent instead of the value.
      return switch (rtx.getKind()) {
        case OBJECT_NAMED_OBJECT -> ((ObjectNamedObjectNode) rtx.getNode()).getPathNodeKey();
        case OBJECT_NAMED_ARRAY -> ((ObjectNamedArrayNode) rtx.getNode()).getPathNodeKey();
        case ARRAY -> ((ImmutableArrayNode) rtx.getNode()).getPathNodeKey();
        default -> 0;
      };
    } finally {
      rtx.moveTo(nodeKey);
    }
  }

}
