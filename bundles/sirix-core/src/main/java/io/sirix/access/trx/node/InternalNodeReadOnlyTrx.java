package io.sirix.access.trx.node;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
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

  /**
   * Apply the logical cursor side effects of {@link #moveTo(long)} after a caller has proved that
   * the current physical node binding can be reused.
   *
   * <p>This is an internal, narrowly scoped hook. It does not resolve or rebind a node and must
   * only be used for an approved physical self-move shortcut.</p>
   */
  void prepareForApprovedSelfMove();

  /**
   * Bind the cursor directly to the document record allocated most recently by {@code writer}.
   *
   * <p>The implementation rejects writer-identity, key, page, and slot-address mismatches before
   * changing logical or physical cursor state. Once those allocation guards pass it may apply the
   * ordinary move prelude while attempting to bind the slot, so callers must always fall back to
   * {@link #moveTo(long)} when it returns {@code false}. This hook is only for the immediate
   * create/link/position sequence of an insertion.</p>
   *
   * @param writer the write transaction's current storage writer
   * @param nodeKey the newly allocated document node key expected by the caller
   * @return {@code true} if the cursor was rebound directly to the fresh allocation
   */
  boolean tryMoveToLastAllocatedDocumentNode(StorageEngineWriter writer, long nodeKey);

  void setPageReadTransaction(StorageEngineReader trx);
}
