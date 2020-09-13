package org.sirix.access.trx.node.xml;

import org.sirix.access.trx.node.AbstractNodeHashing;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.PageTrx;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.page.UnorderedKeyValuePage;

final class XmlNodeHashing extends AbstractNodeHashing {

  private final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx;

  /**
   * Constructor.
   *
   * @param hashType        the hash type used
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param pageWriteTrx    the page trx
   */
  XmlNodeHashing(final HashType hashType, final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx,
      final PageTrx pageWriteTrx) {
    super(hashType, nodeReadOnlyTrx, pageWriteTrx);
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
  protected void setCurrentNode(ImmutableNode node) {
    nodeReadOnlyTrx.setCurrentNode((ImmutableXmlNode) node);
  }

}
