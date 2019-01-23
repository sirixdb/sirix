package org.sirix.access.trx.node.xdm;

import org.sirix.api.PageReadTrx;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableXdmNode;

public interface InternalXdmNodeReadTrx extends XdmNodeReadTrx {
  ImmutableXdmNode getCurrentNode();

  void setCurrentNode(ImmutableXdmNode node);

  StructNode getStructuralNode();

  void assertNotClosed();

  void setPageReadTransaction(PageReadTrx trx);
}
