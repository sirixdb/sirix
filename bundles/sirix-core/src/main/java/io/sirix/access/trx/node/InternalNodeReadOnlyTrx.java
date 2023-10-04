package io.sirix.access.trx.node;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;

public interface InternalNodeReadOnlyTrx<N extends ImmutableNode> extends NodeCursor, NodeReadOnlyTrx {
  N getCurrentNode();

  void setCurrentNode(N node);

  StructNode getStructuralNode();

  void assertNotClosed();

  void setPageReadTransaction(PageReadOnlyTrx trx);
}
