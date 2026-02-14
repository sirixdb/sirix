package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
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
      final StorageEngineWriter pageWriteTrx) {
    super(resourceConfiguration, nodeReadOnlyTrx, pageWriteTrx);
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
  }

  @Override
  protected StructNode getStructuralNode() {
    return nodeReadOnlyTrx.getStructuralNode();
  }

}
