package org.sirix.access.trx.node.xml;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;

public interface InternalXmlNodeReadTrx extends XmlNodeReadOnlyTrx {
  ImmutableXmlNode getCurrentNode();

  void setCurrentNode(ImmutableXmlNode node);

  StructNode getStructuralNode();

  void assertNotClosed();

  void setPageReadTransaction(PageReadOnlyTrx trx);
}
