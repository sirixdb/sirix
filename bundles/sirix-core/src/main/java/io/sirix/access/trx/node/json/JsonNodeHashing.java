package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.access.trx.node.AbstractNodeHashing;

final class JsonNodeHashing extends AbstractNodeHashing<ImmutableNode, JsonNodeReadOnlyTrx> {

  private final InternalJsonNodeReadOnlyTrx nodeReadOnlyTrx;
  private final JsonHashingMutation mutation;

  /**
   * Constructor.
   *
   * @param resourceConfiguration the resource configuration
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param storageEngineWriter the storage engine writer
   */
  JsonNodeHashing(final ResourceConfiguration resourceConfiguration, final InternalJsonNodeReadOnlyTrx nodeReadOnlyTrx,
      final StorageEngineWriter storageEngineWriter) {
    super(resourceConfiguration, nodeReadOnlyTrx, storageEngineWriter);
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
    mutation =
        new JsonHashingMutation(storageEngineWriter, nodeReadOnlyTrx, resourceConfiguration.hashType, getBytes());
  }

  JsonHashingMutation mutation() {
    return mutation;
  }

  @Override
  protected StructNode getStructuralNode() {
    return nodeReadOnlyTrx.getStructuralNodeView();
  }

}
