/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The packed {@code (segment, id)} cell a segment-scoped column stores.
 *
 * <p>
 * The packing is what lets one resolver serve a column whose dictionary is per segment: the cell says
 * which dictionary it belongs to, so nothing downstream has to be told. It is free on disk only
 * because a row group never straddles a boundary — every cell in a leaf then carries the same high
 * bits, and the lane is FOR-packed against the leaf's own minimum.
 * </p>
 */
final class SegmentUnionReadViewTest {

  @Test
  @DisplayName("a cell round-trips its segment and its id, across the whole range each may take")
  void cellsRoundTrip() {
    final int[] segments = {0, 1, 2, 255, 1 << 16, (1 << 24) - 1};
    final int[] ids = {1, 2, 255, 65535, 1 << 20, Integer.MAX_VALUE};
    for (final int segment : segments) {
      for (final int id : ids) {
        final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, id);
        assertEquals(segment, ProjectionIndexRowGroupPage.segmentOfCell(cell),
            "segment of (" + segment + ", " + id + ")");
        assertEquals(id, ProjectionIndexRowGroupPage.idOfCell(cell), "id of (" + segment + ", " + id + ")");
      }
    }
  }

  @Test
  @DisplayName("the same id in two segments is two different cells, which is the whole point")
  void theSameIdInTwoSegmentsDiffers() {
    final long inZero = ProjectionIndexRowGroupPage.packSegmentCell(0, 7);
    final long inOne = ProjectionIndexRowGroupPage.packSegmentCell(1, 7);
    assertNotEquals(inZero, inOne);
    assertEquals(7, ProjectionIndexRowGroupPage.idOfCell(inZero));
    assertEquals(7, ProjectionIndexRowGroupPage.idOfCell(inOne));
    // Segment 0's cells are bare ids, so a single-segment column packs exactly what it would have.
    assertEquals(7L, inZero);
  }

  @Test
  @DisplayName("cells of ONE leaf differ only in their low bits, which is why the packing is free")
  void oneLeafsCellsShareTheirHighBits() {
    // A leaf never straddles a boundary, so its cells all carry the same segment. FOR-packing stores
    // each cell minus the leaf's minimum, and those deltas are exactly the id deltas — the packed
    // width is what it would have been for bare ids, whatever the segment number is.
    final int segment = 4242;
    final long first = ProjectionIndexRowGroupPage.packSegmentCell(segment, 1);
    final long last = ProjectionIndexRowGroupPage.packSegmentCell(segment, 275_494);
    assertEquals(275_493L, last - first, "the span of a leaf's cells is the span of its ids");
    assertTrue(last - first < (1L << 32), "and it never reaches into the segment's bits");
  }
}
