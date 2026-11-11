/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SegmentAccess;
import java.lang.foreign.MemorySegment;

/**
 * A decoded numeric column lane of one leaf living in an off-heap slab: {@code rows} longs at byte
 * offset {@code base} of {@code segment}, eight bytes per row, written by the decoder itself. The
 * segment has the global scope (allocator-owned, freed explicitly by the store), so reads carry no
 * liveness check; kernels keep the segment in a local and read linear offsets through the typed
 * handle ({@link SegmentAccess}).
 */
public final class NumericLane {
  private final MemorySegment segment;
  private final long base;
  private final int rows;

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

}
