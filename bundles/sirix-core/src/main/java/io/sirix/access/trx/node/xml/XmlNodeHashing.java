package io.sirix.access.trx.node.xml;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.access.trx.node.AbstractNodeHashing;

final class XmlNodeHashing extends AbstractNodeHashing<ImmutableXmlNode, XmlNodeReadOnlyTrx> {

  private final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx;

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param storageEngineWriter the storage engine writer
   */
  XmlNodeHashing(final ResourceConfiguration resourceConfig, final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx,
      final StorageEngineWriter storageEngineWriter) {
    super(resourceConfig, nodeReadOnlyTrx, storageEngineWriter);
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
  }

  @Override
  protected StructNode getStructuralNode() {
    return nodeReadOnlyTrx.getStructuralNode();
  }

}
