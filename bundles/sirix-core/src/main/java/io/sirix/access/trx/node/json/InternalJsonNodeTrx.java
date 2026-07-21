package io.sirix.access.trx.node.json;

import io.sirix.api.json.JsonNodeTrx;
import io.sirix.access.trx.node.InternalNodeTrx;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.node.NodeKind;

public interface InternalJsonNodeTrx extends InternalNodeTrx<JsonNodeTrx>, JsonNodeTrx {

  JsonNodeTrx insertStringValueAsFirstChild(byte[] utf8Value);

  JsonNodeTrx insertStringValueAsLastChild(byte[] utf8Value);

  JsonNodeTrx insertStringValueAsLeftSibling(byte[] utf8Value);

  JsonNodeTrx insertStringValueAsRightSibling(byte[] utf8Value);

  JsonNodeTrx insertStringValueAsFirstChild(byte[] buf, int off, int len);

  JsonNodeTrx insertStringValueAsLastChild(byte[] buf, int off, int len);

  JsonNodeTrx insertStringValueAsLeftSibling(byte[] buf, int off, int len);

  JsonNodeTrx insertStringValueAsRightSibling(byte[] buf, int off, int len);

  // ==================== BULK STREAM INSERT (document-order fast lane) ====================
  //
  // Cursor-free append primitives for document-order bulk shredding (docs/BULK_INGESTION.md).
  // The shredder owns the structural frame stack and passes every anchor key explicitly; the
  // transaction performs the identical node creation, neighbor adaptation, hashing calls, and
  // index maintenance as the cursor-based insert methods — it only skips reading and restoring
  // the cursor. All methods may ONLY be called between a successful
  // {@link #tryBeginBulkStreamInsert()} and {@link #endBulkStreamInsert()}.

  /**
   * Try to enter bulk-stream mode. Preconditions (all checked here): fast lane enabled,
   * cursor on an empty document root, no dewey IDs, no per-node primitive indexes, hash
   * mode ROLLING or NONE (POSTORDER reads the cursor inside its per-insert adaptation),
   * no transaction lock, bulk-insert hashing mode active. When any precondition fails
   * the transaction stays untouched and the caller must use the classic cursor-based
   * path. Path statistics need no gate — the bulk methods record them identically.
   *
   * @return {@code true} when bulk-stream mode is active
   */
  default boolean tryBeginBulkStreamInsert() {
    return false;
  }

  /** Leave bulk-stream mode (must be called from a {@code finally}). */
  default void endBulkStreamInsert() {
    throw new UnsupportedOperationException();
  }

  /**
   * Notify the streaming hash/descendant fold that the current container's END token
   * arrived: its child list is final, so the container's subtree fold can be completed
   * and folded into its parent. Must be called exactly once per
   * {@link #bulkInsertObject(long, long)}, {@link #bulkInsertArray(long, long, long)} or
   * structural {@link #bulkInsertObjectRecordStructural(String, NodeKind, long, long, long)}
   * call, in strict LIFO order.
   */
  default void bulkCloseContainer() {
    throw new UnsupportedOperationException();
  }

  /**
   * The path-summary node key assigned to the most recently bulk-inserted container
   * (ARRAY / OBJECT_NAMED_OBJECT / OBJECT_NAMED_ARRAY) — the path context its children
   * resolve against.
   */
  default long getLastBulkPathNodeKey() {
    throw new UnsupportedOperationException();
  }

  /** Append a plain OBJECT (array element / root object). */
  default long bulkInsertObject(long parentKey, long leftSibKey) {
    throw new UnsupportedOperationException();
  }

  /** Append a plain ARRAY (array element / root array); resolves its path node. */
  default long bulkInsertArray(long parentKey, long leftSibKey, long parentPathNodeKey) {
    throw new UnsupportedOperationException();
  }

  /**
   * Append a fused structural object field ({@code "name": {…}} or {@code "name": […]}) —
   * an OBJECT_NAMED_OBJECT or OBJECT_NAMED_ARRAY record.
   *
   * @param valueKind {@link NodeKind#OBJECT} or {@link NodeKind#ARRAY}
   */
  default long bulkInsertObjectRecordStructural(String name, NodeKind valueKind, long parentKey,
      long leftSibKey, long parentPathNodeKey) {
    throw new UnsupportedOperationException();
  }

  /** Append a fused primitive object field — an OBJECT_NAMED_STRING/NUMBER/BOOLEAN/NULL record. */
  default long bulkInsertObjectRecordPrimitive(String name, ObjectRecordValue<?> value,
      long parentKey, long leftSibKey, long parentPathNodeKey) {
    throw new UnsupportedOperationException();
  }

  /** Append a STRING_VALUE array element. */
  default long bulkInsertStringValue(String value, long parentKey, long leftSibKey,
      long parentPathNodeKey) {
    throw new UnsupportedOperationException();
  }

  /** Append a NUMBER_VALUE array element. */
  default long bulkInsertNumberValue(Number value, long parentKey, long leftSibKey,
      long parentPathNodeKey) {
    throw new UnsupportedOperationException();
  }

  /** Append a BOOLEAN_VALUE array element. */
  default long bulkInsertBooleanValue(boolean value, long parentKey, long leftSibKey,
      long parentPathNodeKey) {
    throw new UnsupportedOperationException();
  }

  /** Append a NULL_VALUE array element. */
  default long bulkInsertNullValue(long parentKey, long leftSibKey, long parentPathNodeKey) {
    throw new UnsupportedOperationException();
  }
}
