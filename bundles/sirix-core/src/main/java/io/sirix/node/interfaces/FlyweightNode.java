package io.sirix.node.interfaces;

import java.lang.foreign.MemorySegment;

/**
 * Interface for nodes that support LeanStore-style flyweight binding to a slotted page MemorySegment.
 *
 * <p>Flyweight nodes can serialize themselves directly to a page heap (with per-record offset tables)
 * and bind to an existing record in the heap for direct in-place reads/writes without Java object
 * intermediation.</p>
 *
 * <p>When bound, all getters/setters operate directly on the page MemorySegment via the offset table.
 * When unbound, they operate on Java primitive fields (normal mode).</p>
 */
public interface FlyweightNode extends DataRecord {

  /**
   * Serialize this node to the target MemorySegment in the slotted page heap format:
   * {@code [nodeKind:1][fieldOffsets:N×1byte][varint fields + hash + payload]}.
   *
   * <p>All Java primitive fields must be materialized before calling this method
   * (i.e., if the node has lazy fields, they must be parsed first).</p>
   *
   * @param target the target MemorySegment to write to
   * @param offset the absolute byte offset to start writing at
   * @return the total number of bytes written
   */
  int serializeToHeap(MemorySegment target, long offset);

  /**
   * Bind this node as a flyweight to a page MemorySegment.
   * After binding, all getters/setters read/write directly to page memory via the offset table.
   *
   * @param page       the page MemorySegment
   * @param recordBase absolute byte offset of this record in the page
   * @param nodeKey    the node key (for delta decoding)
   * @param slotIndex  the slot index in the page directory
   */
  void bind(MemorySegment page, long recordBase, long nodeKey, int slotIndex);

  /**
   * Unbind from page memory and materialize all fields into Java primitives.
   * After unbind, the node operates in primitive mode.
   */
  void unbind();

  /**
   * Check if this node is currently bound to a page MemorySegment.
   *
   * @return true if bound (flyweight mode), false if operating on Java primitives
   */
  boolean isBound();

  /**
   * Check if this node is bound to a specific page MemorySegment.
   * Used to detect cross-page bindings (e.g., bound to complete page but need to rebind to modified page).
   *
   * @param page the page MemorySegment to check against
   * @return true if bound to the specified page
   */
  boolean isBoundTo(MemorySegment page);

  /**
   * Get the slot index this node is currently bound to.
   * Only valid when {@link #isBound()} is true.
   *
   * @return the slot index in the page directory
   */
  int getSlotIndex();

  /**
   * Estimate the serialized size of this record in bytes.
   * Used to ensure the slotted page has enough space before serialization.
   * Structural nodes return a small constant; value nodes add their payload size.
   *
   * @return conservative upper bound on serialized byte count
   */
  default int estimateSerializedSize() {
    return 256;
  }

  /**
   * Check if this node is a write-path singleton managed by a node factory.
   * Write singletons are rebound per-access and must NOT be stored in records[].
   *
   * @return true if this is a factory-managed write singleton
   */
  default boolean isWriteSingleton() {
    return false;
  }

  /**
   * Mark this node as a write-path singleton (or clear the mark).
   *
   * @param writeSingleton true to mark as write singleton
   */
  default void setWriteSingleton(boolean writeSingleton) {
    // Default no-op; concrete types override
  }
}
