package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.PageTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.access.trx.node.AbstractNodeHashing;

final class JsonNodeHashing extends AbstractNodeHashing<ImmutableNode, JsonNodeReadOnlyTrx> {

  private final InternalJsonNodeReadOnlyTrx nodeReadOnlyTrx;

  /**
   * Constructor.
   *
   * @param resourceConfiguration the resource configuration
   * @param nodeReadOnlyTrx       the internal read-only node trx
   * @param pageWriteTrx          the page trx
   */
  JsonNodeHashing(final ResourceConfiguration resourceConfiguration, final InternalJsonNodeReadOnlyTrx nodeReadOnlyTrx,
      final PageTrx pageWriteTrx) {
    super(resourceConfiguration, nodeReadOnlyTrx, pageWriteTrx);
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
  }

  @Override
  protected StructNode getStructuralNode() {
    return nodeReadOnlyTrx.getStructuralNode();
  }

  @Override
  protected ImmutableNode getCurrentNode() {
    return nodeReadOnlyTrx.getCurrentNode();
  }

  @Override
  protected void setCurrentNode(final ImmutableNode node) {
    nodeReadOnlyTrx.setCurrentNode(node);
  }

}
