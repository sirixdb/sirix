package org.sirix.access.trx.node.xdm;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableXdmNode;

public interface InternalXdmNodeReadTrx extends XdmNodeReadOnlyTrx {
  ImmutableXdmNode getCurrentNode();

  void setCurrentNode(ImmutableXdmNode node);

  StructNode getStructuralNode();

  void assertNotClosed();

  void setPageReadTransaction(PageReadOnlyTrx trx);
}
