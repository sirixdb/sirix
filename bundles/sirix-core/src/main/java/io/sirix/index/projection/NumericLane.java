/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SegmentAccess;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;

/**
 * A decoded numeric column lane of one leaf living in an off-heap slab: {@code rows} longs at byte
 * offset {@code base} of {@code segment}, eight bytes per row, written by the decoder itself. The
 * segment has the global scope (allocator-owned, freed explicitly by the store), so reads carry no
 * liveness check; kernels keep the segment in a local and read linear offsets through the typed
 * handle ({@link SegmentAccess}).
 *
 * <p>
 * {@link #values()} is the transition seam for consumers that still take an array: it copies the
 * lane to the heap once per slice and counts the copy, so a measurement can report which paths were
 * not converted. Converted kernels never call it.
 */
public final class NumericLane {
  private static final LongAdder MATERIALIZED = new LongAdder();
  private static final LongAdder MATERIALIZED_BYTES = new LongAdder();

  private final MemorySegment segment;
  private final long base;
  private final int rows;
  private volatile long @Nullable [] heap;

  public NumericLane(final MemorySegment segment, final long base, final int rows) {
    if (base < 0 || rows < 0 || base + 8L * rows > segment.byteSize()) {
      throw new IllegalArgumentException(
          "lane [" + base + ", +" + rows + " rows) outside a slab of " + segment.byteSize() + " bytes");
    }
    this.segment = segment;
    this.base = base;
    this.rows = rows;
  }

  /** The slab holding the lane. */
  public MemorySegment segment() {
    return segment;
  }

  /** Byte offset of row 0 within {@link #segment()}. */
  public long base() {
    return base;
  }

  public int rows() {
    return rows;
  }

  /** The value of {@code row}; kernels hoist {@link #segment()} and {@link #base()} instead. */
  public long get(final int row) {
    return SegmentAccess.getLong(segment, base + ((long) row << 3));
  }

  /** A heap copy of the lane, made once and counted; see the class comment. */
  public long[] values() {
    long[] copy = heap;
    if (copy == null) {
      synchronized (this) {
        copy = heap;
        if (copy == null) {
          copy = new long[rows];
          MemorySegment.copy(segment, ValueLayout.JAVA_LONG_UNALIGNED, base, copy, 0, rows);
          MATERIALIZED.increment();
          MATERIALIZED_BYTES.add(8L * rows);
          heap = copy;
        }
      }
    }
    return copy;
  }

  /** Lanes copied to the heap by {@link #values()}, process-wide (measurement observability). */
  public static long materializedCount() {
    return MATERIALIZED.sum();
  }

  /** Bytes copied to the heap by {@link #values()}, process-wide. */
  public static long materializedBytes() {
    return MATERIALIZED_BYTES.sum();
  }
}
