package org.sirix.access.trx.node;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;

public interface InternalNodeReadOnlyTrx extends JsonNodeReadOnlyTrx {
  ImmutableNode getCurrentNode();

  void setCurrentNode(ImmutableNode node);

  StructNode getStructuralNode();

  void assertNotClosed();

  void setPageReadTransaction(PageReadOnlyTrx trx);
}
