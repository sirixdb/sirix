/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * One scratch buffer of an allocator: its backing, the views over it, and the counters of the
 * borrows it served. Owned by its {@link ScratchSource} for its whole life and lent to one
 * {@link ScratchLease} at a time, so the views are built once and a warm borrow allocates nothing.
 *
 * <p>
 * The {@link #era} is the ownership protocol: even while the source holds the buffer, odd while a
 * lease does. Lending moves it to the next odd value; a return must name that exact value and moves
 * it to the next even one with a compare-and-set, so a return that comes twice, or after the buffer
 * was lent again, changes nothing.
 *
 * <p>
 * The counters are written only by the thread that holds the buffer and read by
 * {@link ScratchStats} snapshots from any thread, hence the opaque accesses through
 * {@link ScratchHandles}: no fence on the borrow path, and no counter shared between borrowers.
 */
final class ScratchCarrier {

  /** The source that owns this buffer and takes it back. */
  final ScratchSource source;

  /** Index of the capacity in the source's tables. */
  final int capacityClass;

  /** The whole buffer: a view that carries no per-access liveness check of its own. */
  final MemorySegment segment;

  /** {@code segment.byteSize()}. */
  final long capacity;

  /** Frame slot backing this buffer, or {@code -1} when a region of its own backs it. */
  final int slotIndex;

  /** The slot's version while this buffer owns it; meaningless without a slot. */
  final long slotVersion;

  /** Links of the source's list of live buffers; guarded by the source's registry lock. */
  @Nullable
  ScratchCarrier previous;
  @Nullable
  ScratchCarrier next;

  /** Accessed through {@link ScratchHandles#ERA}. */
  long era;

  /** Exclusive aggregation grant while lent; null for ordinary scratch and every idle buffer. */
  @Nullable
  AggregationGrant aggregationGrant;

  /** Accessed through {@link ScratchHandles#ACQUISITIONS}. */
  long acquisitions;

  /** Accessed through {@link ScratchHandles#REQUESTED_BYTES}. */
  long requestedBytes;

  private final long[] useAcquisitions = new long[ScratchUse.COUNT];
  private final long[] useRequestedBytes = new long[ScratchUse.COUNT];

  /** The buffer view, built on the first borrow that asks for one. */
  private @Nullable ByteBuffer buffer;

  ScratchCarrier(final ScratchSource source, final int capacityClass, final MemorySegment segment, final int slotIndex,
      final long slotVersion) {
    this.source = source;
    this.capacityClass = capacityClass;
    this.segment = segment;
    this.capacity = segment.byteSize();
    this.slotIndex = slotIndex;
    this.slotVersion = slotVersion;
  }

  /**
   * Lend this buffer: called by the source once it holds the buffer exclusively.
   *
   * @return the era of this borrow, to be named by its return
   */
  long lend(final long requested, final ScratchUse use) {
    final long lent = (long) ScratchHandles.ERA.getOpaque(this) + 1L;
    assert (lent & 1L) == 1L : "a buffer is lent from an even era";
    final int useIndex = use.ordinal();
    ScratchHandles.ACQUISITIONS.setOpaque(this, (long) ScratchHandles.ACQUISITIONS.getOpaque(this) + 1L);
    ScratchHandles.REQUESTED_BYTES.setOpaque(this, (long) ScratchHandles.REQUESTED_BYTES.getOpaque(this) + requested);
    ScratchHandles.COUNTER.setOpaque(useAcquisitions, useIndex,
        (long) ScratchHandles.COUNTER.getOpaque(useAcquisitions, useIndex) + 1L);
    ScratchHandles.COUNTER.setOpaque(useRequestedBytes, useIndex,
        (long) ScratchHandles.COUNTER.getOpaque(useRequestedBytes, useIndex) + requested);
    final ByteBuffer view = buffer;
    if (view != null) {
      view.clear().order(ByteOrder.LITTLE_ENDIAN);
    }
    // Published last: a snapshot that sees the odd era sees a buffer that is lent.
    ScratchHandles.ERA.setRelease(this, lent);
    return lent;
  }

  /**
   * Take this buffer back from the borrow of {@code lentEra}.
   *
   * @return whether that borrow still held the buffer
   */
  boolean takeBack(final long lentEra) {
    return (lentEra & 1L) == 1L && ScratchHandles.ERA.compareAndSet(this, lentEra, lentEra + 1L);
  }

  /** Whether a lease holds this buffer now. */
  boolean isLent() {
    return ((long) ScratchHandles.ERA.getAcquire(this) & 1L) == 1L;
  }

  /**
   * The little-endian buffer view over the whole buffer, cleared for each borrow. Its cursor belongs
   * to the lease that holds the buffer.
   *
   * @throws UnsupportedOperationException when the buffer is larger than a {@link ByteBuffer} can
   *         address
   */
  ByteBuffer buffer() {
    ByteBuffer view = buffer;
    if (view == null) {
      if (capacity > Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(
            "a scratch buffer of " + capacity + " bytes has no ByteBuffer view; use its segment");
      }
      view = segment.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
      buffer = view;
    }
    return view;
  }

  long acquisitionCount() {
    return (long) ScratchHandles.ACQUISITIONS.getOpaque(this);
  }

  long requestedByteCount() {
    return (long) ScratchHandles.REQUESTED_BYTES.getOpaque(this);
  }

  /** Add this buffer's per-use counters to the given totals. */
  void addUseCountersTo(final long[] acquisitionsByUse, final long[] requestedBytesByUse) {
    for (int use = 0; use < ScratchUse.COUNT; use++) {
      acquisitionsByUse[use] += (long) ScratchHandles.COUNTER.getOpaque(useAcquisitions, use);
      requestedBytesByUse[use] += (long) ScratchHandles.COUNTER.getOpaque(useRequestedBytes, use);
    }
  }
}
