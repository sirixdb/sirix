package io.sirix.access.trx.node.xml;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractNodeHashing;
import io.sirix.api.PageTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;

final class XmlNodeHashing extends AbstractNodeHashing<ImmutableXmlNode, XmlNodeReadOnlyTrx> {

  private final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx;
  private final ResourceConfiguration resourceConfiguration;
  private final PageTrx pageTrx;

  /**
   * Constructor.
   *
   * @param resourceConfiguration the resource configuration
   * @param nodeReadOnlyTrx       the internal read-only node trx
   * @param pageTrx          the page trx
   */
  XmlNodeHashing(final ResourceConfiguration resourceConfiguration, final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx,
      final PageTrx pageTrx) {
    super(resourceConfiguration, nodeReadOnlyTrx, pageTrx);
    this.resourceConfiguration = resourceConfiguration;
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
    this.pageTrx = pageTrx;
  }

  @Override
  public XmlNodeHashing clone() {
    final var xmlNodeHashing = new XmlNodeHashing(resourceConfiguration, nodeReadOnlyTrx, pageTrx);
    xmlNodeHashing.setAutoCommit(isAutoCommitting());
    xmlNodeHashing.setBulkInsert(isBulkInsert());
    return xmlNodeHashing;
  }

  @Override
  protected StructNode getStructuralNode() {
    return nodeReadOnlyTrx.getStructuralNode();
  }

  @Override
  protected ImmutableXmlNode getCurrentNode() {
    return nodeReadOnlyTrx.getCurrentNode();
  }

  @Override
  protected void setCurrentNode(final ImmutableXmlNode node) {
    nodeReadOnlyTrx.setCurrentNode(node);
  }

}
