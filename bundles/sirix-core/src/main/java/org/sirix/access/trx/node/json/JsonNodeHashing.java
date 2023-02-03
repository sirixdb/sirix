package org.sirix.access.trx.node.json;

import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.AbstractNodeHashing;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;

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
