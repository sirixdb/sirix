package org.sirix.access.trx.node.xml;

import org.sirix.api.xml.XmlNodeTrx;

public interface InternalXmlNodeTrx {
  XmlNodeTrx setBulkInsertion(boolean bulkInsertion);

  void adaptHashesInPostorderTraversal();
}
