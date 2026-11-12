/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.lang.ref.Reference;
import java.util.Objects;

/**
 * Checked lane access to allocator-owned aggregation memory. Every entry validates that the buffer
 * is live and that the complete range lies inside it; no public method accepts an address. The
 * aggregation kernels use a separately audited accessor after resolving a complete record
 * ({@link GroupKernelLanes}).
 *
 * <p>
 * A single-lane access costs what an array access costs: one liveness test and one range check
 * against the buffer, made explicitly, and then a load or store at the checked address. The
 * buffer's bounded segment is not consulted per lane because its own check cannot be hoisted from
 * the data-dependent addresses a group table is read at; it remains the authority the buffer's
 * bounds were taken from, and debug mode ({@link GroupKernelLanes#DEBUG}) reads and writes through
 * it. The single-lane methods keep to the size {@link GroupKernelLanes} explains, for the same
 * reason.
 */
public final class GroupLanes {

  private static final VarHandle LANE = ValueLayout.JAVA_LONG_UNALIGNED.varHandle();

  /**
   * Debug mode's atomic lane accesses. Declared here, not beside their one user
   * {@link GroupDebugRecords}: a segment handle is a constant of a native image only in a class
   * initialized at build time, which this class is and one with per-thread state cannot be.
   */
  private static final VarHandle ATOMIC_LANE = ValueLayout.JAVA_LONG.varHandle();

  private GroupLanes() {}

  /** Lane {@code lane} of the bounded view {@code view}, read opaquely. */
  static long boundedGetOpaque(final GroupBuffer view, final int lane) {
    view.requireLive();
    final long result = (long) ATOMIC_LANE.getOpaque(view.segment, (long) lane << 3);
    Reference.reachabilityFence(view);
    return result;
  }

  /** Compare-and-exchange on lane {@code lane} of the bounded view {@code view}. */
  static long boundedCompareAndExchange(final GroupBuffer view, final int lane, final long expected, final long value) {
    view.requireLive();
    final long result = (long) ATOMIC_LANE.compareAndExchange(view.segment, (long) lane << 3, expected, value);
    Reference.reachabilityFence(view);
    return result;
  }

  /** Atomic add to lane {@code lane} of the bounded view {@code view}; the value before it. */
  static long boundedGetAndAdd(final GroupBuffer view, final int lane, final long value) {
    view.requireLive();
    final long result = (long) ATOMIC_LANE.getAndAdd(view.segment, (long) lane << 3, value);
    Reference.reachabilityFence(view);
    return result;
  }

  /** Lane {@code lane} of {@code lanes}. */
  public static long get(final GroupBuffer lanes, final int lane) {
    if (GroupKernelLanes.DEBUG) {
      return boundedGet(lanes, lane);
    }
    final long result = GroupKernelLanes.load(lanes.laneAddress(lane));
    Reference.reachabilityFence(lanes);
    return result;
  }

  /** Store {@code value} in lane {@code lane} of {@code lanes}. */
  public static void set(final GroupBuffer lanes, final int lane, final long value) {
    if (GroupKernelLanes.DEBUG) {
      boundedSet(lanes, lane, value);
      return;
    }
    GroupKernelLanes.store(lanes.laneAddress(lane), value);
    Reference.reachabilityFence(lanes);
  }

  /** Add {@code delta} to lane {@code lane} of {@code lanes} (wrapping, like {@code +=}). */
  public static void add(final GroupBuffer lanes, final int lane, final long delta) {
    if (GroupKernelLanes.DEBUG) {
      boundedSet(lanes, lane, boundedGet(lanes, lane) + delta);
      return;
    }
    GroupKernelLanes.addAt(lanes.laneAddress(lane), delta);
    Reference.reachabilityFence(lanes);
  }

  /** Add one to lane {@code lane} of {@code lanes}. */
  public static void increment(final GroupBuffer lanes, final int lane) {
    add(lanes, lane, 1L);
  }

  /** Debug mode's read: through the buffer's bounded segment, generation checked. */
  private static long boundedGet(final GroupBuffer lanes, final int lane) {
    lanes.checkRange(lane, 1);
    final long result = (long) LANE.get(lanes.segment, (long) (lane - lanes.segmentFirstLane) << 3);
    Reference.reachabilityFence(lanes);
    return result;
  }

  /** Debug mode's write: through the buffer's bounded segment, generation checked. */
  private static void boundedSet(final GroupBuffer lanes, final int lane, final long value) {
    lanes.checkRange(lane, 1);
    LANE.set(lanes.segment, (long) (lane - lanes.segmentFirstLane) << 3, value);
    Reference.reachabilityFence(lanes);
  }

  /** Lanes {@code lanes} holds: its size in longs. */
  public static int count(final GroupBuffer lanes) {
    lanes.requireLive();
    return lanes.lanes;
  }

  /**
   * Copy {@code count} lanes, with both complete ranges checked before the bulk copy.
   */
  public static void copy(final GroupBuffer source, final int sourceLane, final GroupBuffer target,
      final int targetLane, final int count) {
    source.checkRange(sourceLane, count);
    target.checkRange(targetLane, count);
    MemorySegment.copy(source.segment, (long) (sourceLane - source.segmentFirstLane) << 3, target.segment,
        (long) (targetLane - target.segmentFirstLane) << 3, (long) count << 3);
    Reference.reachabilityFence(source);
    Reference.reachabilityFence(target);
  }

  /**
   * Copy {@code count} lanes of a heap array (kernel scratch, a key's identity) into {@code target}.
   */
  public static void copy(final long[] source, final int sourceIndex, final GroupBuffer target, final int targetLane,
      final int count) {
    Objects.checkFromIndexSize(sourceIndex, count, source.length);
    target.checkRange(targetLane, count);
    MemorySegment.copy(source, sourceIndex, target.segment, ValueLayout.JAVA_LONG_UNALIGNED,
        (long) (targetLane - target.segmentFirstLane) << 3, count);
    Reference.reachabilityFence(target);
  }

  /** Set the lanes {@code [fromLane, toLane)} of {@code lanes} to {@code value}. */
  public static void fill(final GroupBuffer lanes, final int fromLane, final int toLane, final long value) {
    if (toLane < fromLane) {
      throw new IndexOutOfBoundsException("reversed aggregation fill range");
    }
    lanes.checkRange(fromLane, toLane - fromLane);
    for (long at = (long) (fromLane - lanes.segmentFirstLane) << 3,
        end = (long) (toLane - lanes.segmentFirstLane) << 3; at < end; at += Long.BYTES) {
      LANE.set(lanes.segment, at, value);
    }
    Reference.reachabilityFence(lanes);
  }

  /** A heap copy of {@code count} lanes from {@code lane} on: results leaving a scan, and tests. */
  public static long[] toArray(final GroupBuffer lanes, final int lane, final int count) {
    lanes.checkRange(lane, count);
    final long[] copy = new long[count];
    MemorySegment.copy(lanes.segment, ValueLayout.JAVA_LONG_UNALIGNED, (long) (lane - lanes.segmentFirstLane) << 3,
        copy, 0, count);
    Reference.reachabilityFence(lanes);
    return copy;
  }
}
