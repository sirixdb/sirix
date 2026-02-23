package io.sirix.access.trx.node;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;

public interface InternalNodeReadOnlyTrx<N extends ImmutableNode> extends NodeCursor, NodeReadOnlyTrx {
  N getCurrentNode();

  void setCurrentNode(N node);

  StructNode getStructuralNode();

  /**
   * Returns a live view of the current structural node without allocating a snapshot.
   * When the cursor is in singleton mode, returns the singleton directly (zero-alloc).
   * The returned reference must NOT be retained across moveTo/prepareRecordForModification
   * calls — extract needed values into local primitives immediately.
   */
  StructNode getStructuralNodeView();

  void assertNotClosed();

  void setPageReadTransaction(StorageEngineReader trx);
}
