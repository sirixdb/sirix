package org.sirix.access.trx.node.xml;

import org.sirix.api.xml.XmlNodeTrx;

public interface InternalXmlNodeTrx extends XmlNodeTrx {
  XmlNodeTrx setBulkInsertion(boolean bulkInsertion);

  void adaptHashesInPostorderTraversal();
}
