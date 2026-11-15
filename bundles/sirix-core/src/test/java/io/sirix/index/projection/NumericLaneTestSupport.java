/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.jspecify.annotations.Nullable;

public final class NumericLaneTestSupport {
  private NumericLaneTestSupport() {}

  public static long @Nullable [] copyNumericValues(final ColumnSlice slice) {
    if (!slice.hasNumericLane()) {
      return null;
    }
    final long[] values = new long[slice.rowCount()];
    MemorySegment.copy(slice.laneSegment(), ValueLayout.JAVA_LONG_UNALIGNED, slice.laneBase(), values, 0,
        values.length);
    return values;
  }

  public static long numericValue(final ColumnSlice slice, final int row) {
    return SegmentAccess.getLong(slice.laneSegment(), slice.laneBase() + 8L * row);
  }
}
