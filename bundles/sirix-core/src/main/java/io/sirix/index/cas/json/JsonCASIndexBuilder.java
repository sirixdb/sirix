package io.sirix.index.cas.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.cas.CASIndexBuilder;
import io.sirix.node.NodeKind;
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
final class JsonCASIndexBuilder extends AbstractJsonNodeVisitor {

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

  private long getPathClassRecord(ImmutableNode node) {
    rtx.moveTo(node.getParentKey());

    final long pcr;

    // Phase 4: parent of a primitive value-node is now exclusively one of the fused-named
    // kinds (52/53) or ARRAY. Dispatch on the concrete NodeKind to pick the right
    // pathNodeKey accessor without alloc.
    final NodeKind kind = rtx.getKind();
    if (kind == NodeKind.OBJECT_NAMED_OBJECT) {
      pcr = ((ObjectNamedObjectNode) rtx.getNode()).getPathNodeKey();
    } else if (kind == NodeKind.OBJECT_NAMED_ARRAY) {
      pcr = ((ObjectNamedArrayNode) rtx.getNode()).getPathNodeKey();
    } else if (kind == NodeKind.ARRAY) {
      pcr = ((ImmutableArrayNode) rtx.getNode()).getPathNodeKey();
    } else {
      pcr = 0;
    }

    return pcr;
  }

}
