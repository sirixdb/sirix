/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * An owned allocation of aggregation lanes. Only a {@link GroupWorkspace} can create one, and a
 * buffer retains that workspace even when a self-returning table itself becomes unreachable.
 *
 * <p>
 * Lane access from outside the aggregation kernels goes through {@link GroupLanes}, which checks
 * every access against this buffer: that it is live, and that the lane lies inside it. The kernels
 * instead resolve a complete <em>record</em> once ({@link #record}, {@link #accumulator}) and then
 * address its lanes without further checks through {@link GroupKernelLanes}; the resolution is
 * where the bound is established, so it validates the whole footprint the kernel is about to touch.
 *
 * <p>
 * The bounded {@link #segment} is the authority for what this buffer covers: its address and size
 * are validated when the buffer is created and every later check is made against them. A returned
 * chunk is permanently invalid, including after the workspace hands the same memory out again.
 */
public final class GroupBuffer {
  /** The bounded segment of this buffer: its allocation, or in debug mode one record of it. */
  final MemorySegment segment;
  final GroupWorkspace owner;
  /** Address of lane {@code 0} of the allocation. */
  final long address;
  /** Lanes the allocation holds. */
  final int lanes;
  final long generation;
  private final int recordStride;
  /**
   * {@code floor(2^64 / recordStride) + 1}: turns {@code lane % recordStride} into two multiplies.
   */
  private final long strideMagic;
  private final int accumulatorOffset;
  private final int accumulatorLanes;
  /** The allocation lane {@link #segment} starts at: {@code 0} except for a debug record view. */
  final int segmentFirstLane;
  private final GroupBuffer allocation;
  private final int firstLane;
  private final int endLane;
  private boolean returned;

  GroupBuffer(final MemorySegment segment, final GroupWorkspace owner, final long generation) {
    this(segment, owner, generation, (int) (segment.byteSize() >>> 3), 0, (int) (segment.byteSize() >>> 3));
  }

  GroupBuffer(final MemorySegment segment, final GroupWorkspace owner, final long generation, final int recordStride,
      final int accumulatorOffset, final int accumulatorLanes) {
    final long bytes = segment.byteSize();
    final long start = segment.address();
    // The mask test also establishes lane alignment and that no lane address can overflow: it is
    // what lets the kernels address this allocation through the unbounded segment.
    if (!segment.isNative() || segment.isReadOnly() || !segment.scope().isAlive() || bytes <= 0 || (bytes & 7L) != 0
        || start <= 0 || (start & GroupKernelLanes.ADDRESS_MASK) != start
        || bytes > GroupKernelLanes.ADDRESS_MASK - start || (bytes >>> 3) > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("invalid aggregation allocation");
    }
    if (recordStride <= 0 || (bytes >>> 3) % recordStride != 0 || accumulatorOffset < 0 || accumulatorLanes < 0
        || accumulatorLanes > recordStride || accumulatorOffset > recordStride - accumulatorLanes) {
      throw new IllegalArgumentException("invalid aggregation record layout");
    }
    this.segment = segment;
    this.owner = Objects.requireNonNull(owner, "owner");
    this.address = start;
    this.lanes = (int) (bytes >>> 3);
    this.generation = generation;
    this.recordStride = recordStride;
    this.strideMagic = Long.divideUnsigned(-1L, recordStride) + 1L;
    this.accumulatorOffset = accumulatorOffset;
    this.accumulatorLanes = accumulatorLanes;
    this.segmentFirstLane = 0;
    this.allocation = this;
    this.firstLane = 0;
    this.endLane = lanes;
  }

  /** Debug mode's view of the lanes {@code [first, first + count)} of {@code source}'s allocation. */
  private GroupBuffer(final GroupBuffer source, final int first, final int count) {
    this.segment = source.allocation.segment.asSlice((long) first << 3, (long) count << 3);
    this.owner = source.owner;
    this.address = source.address;
    this.lanes = source.lanes;
    this.generation = source.generation;
    this.recordStride = source.recordStride;
    this.strideMagic = source.strideMagic;
    this.accumulatorOffset = source.accumulatorOffset;
    this.accumulatorLanes = source.accumulatorLanes;
    this.segmentFirstLane = first;
    this.allocation = source.allocation;
    this.firstLane = first;
    this.endLane = first + count;
  }

  /**
   * Resolve the record {@code [first, first + count)} for {@link GroupKernelLanes}: this buffer is
   * live and holds the complete range. The answer names the range, lane {@code 0} being
   * {@code first}; it is valid until the buffer is handed back, and a kernel must keep this buffer
   * reachable for as long as it uses the record. A block that is never handed back can therefore be
   * resolved once for a whole scan, while a record of a table's chunk is resolved again after every
   * acquisition that may have grown the table.
   *
   * @throws IllegalStateException if the buffer was handed back or its workspace is closed
   * @throws IndexOutOfBoundsException if the range does not lie inside this buffer
   */
  long record(final int first, final int count) {
    checkRange(first, count);
    return GroupKernelLanes.DEBUG
        ? GroupDebugRecords.open(this, first, count)
        : address + ((long) first << 3);
  }

  /**
   * Debug mode's bounded view of {@code [first, first + count)}, addressed by the same lanes as this
   * buffer: checked access through it cannot leave the range.
   */
  GroupBuffer view(final int first, final int count) {
    checkRange(first, count);
    return new GroupBuffer(this, first, count);
  }

  /**
   * Resolve the accumulator a fold of {@code columns} aggregate columns of {@code lanesPerColumn}
   * lanes each writes: {@code [count, firstSeen]} and the columns, starting at {@code first}. Beyond
   * {@link #record}'s range check, {@code first} must be where this buffer's layout puts an
   * accumulator and the fold must fit the accumulator the layout declares, so a fold that was handed
   * a wrong offset or column count fails here instead of writing a neighbouring group.
   *
   * @throws IllegalArgumentException if the column count cannot describe an accumulator
   * @throws IndexOutOfBoundsException if the fold does not fit the layout or the buffer
   * @throws IllegalStateException if the buffer was handed back or its workspace is closed
   */
  long accumulator(final int first, final int columns, final int lanesPerColumn) {
    if (lanesPerColumn <= 0 || columns < 0 || columns > (Integer.MAX_VALUE - 2) / lanesPerColumn) {
      throw new IllegalArgumentException("invalid aggregation column count: " + columns);
    }
    final int count = 2 + columns * lanesPerColumn;
    if (first < 0 || strideRemainder(first) != accumulatorOffset || count > accumulatorLanes) {
      throw new IndexOutOfBoundsException("fold does not fit the aggregation record layout");
    }
    return record(first, count);
  }

  /**
   * {@code lane % recordStride} for a non-negative lane, without a division: exact for every pair of
   * 32-bit operands (Lemire, Kaser, Kurz: "Faster remainder by direct computation", 2019).
   */
  private int strideRemainder(final int lane) {
    return (int) Math.unsignedMultiplyHigh(strideMagic * lane, recordStride);
  }

  /**
   * The address of lane {@code lane} for one checked access outside debug mode: this buffer is live
   * and holds the lane.
   */
  long laneAddress(final int lane) {
    requireLive();
    return address + ((long) Objects.checkIndex(lane, lanes) << 3);
  }

  /** Live, and {@code [first, first + count)} lies inside this buffer; subtraction-based. */
  void checkRange(final int first, final int count) {
    requireLive();
    if (first < firstLane || count < 0 || count > endLane - first) {
      throw new IndexOutOfBoundsException(
          "aggregation range [" + first + ", +" + count + ") outside [" + firstLane + ", " + endLane + ")");
    }
  }

  // Sonar S2178: the eager OR is deliberate: two plain loads and one branch, where the short-circuit
  // form adds a branch to every lane access.
  @SuppressWarnings("java:S2178")
  void requireLive() {
    // Plain reads: closing or handing back never races an access (GroupWorkspace's contract), and a
    // volatile read here would keep the compiler from hoisting anything across a lane access.
    if ((returned | owner.ended) || (GroupKernelLanes.DEBUG && staleView())) {
      throw new IllegalStateException("aggregation buffer belongs to a closed or returned generation");
    }
  }

  /** Whether the allocation this debug view was cut from has been handed back since. */
  private boolean staleView() {
    return allocation.returned || generation != allocation.generation;
  }

  void retire(final GroupWorkspace workspace) {
    if (workspace != owner || allocation != this) {
      throw new IllegalArgumentException("only the owning workspace may return a complete chunk");
    }
    requireLive();
    returned = true;
  }

  /** The allocation this buffer is, or in debug mode is a view of. */
  GroupBuffer allocation() {
    return allocation;
  }

  /** Whether this buffer may still be accessed: {@link #requireLive}'s test, as a question. */
  // Sonar S2178: the eager OR is deliberate: two plain loads and one branch, where the short-circuit
  // form adds a branch to every lane access.
  @SuppressWarnings("java:S2178")
  boolean isLive() {
    return !((returned | owner.ended) || (GroupKernelLanes.DEBUG && staleView()));
  }

  /** First allocation lane this buffer covers: {@code 0} except for a debug record view. */
  int firstLane() {
    return firstLane;
  }

  /** Lanes this buffer covers: the allocation's, or a debug record view's. */
  int viewLanes() {
    return endLane - firstLane;
  }

  /** Aggregation buffers always have native backing. No address is exposed publicly. */
  // Sonar S3400: part of the buffer's public face: callers ask the buffer about its backing instead
  // of assuming it.
  @SuppressWarnings("java:S3400")
  public boolean isNative() {
    return true;
  }
}
