package org.sirix.access.trx.node.json;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;

public interface InternalJsonNodeReadOnlyTrx extends JsonNodeReadOnlyTrx {
  ImmutableJsonNode getCurrentNode();

  void setCurrentNode(ImmutableJsonNode node);

  StructNode getStructuralNode();

  void assertNotClosed();

  void setPageReadTransaction(PageReadOnlyTrx trx);
}
