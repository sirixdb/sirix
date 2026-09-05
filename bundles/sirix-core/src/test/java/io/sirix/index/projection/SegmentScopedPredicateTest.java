/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code = lit} over a segment-scoped column: one literal, a different id per segment, and a leaf
 * that answers from its own segment's entry.
 */
final class SegmentScopedPredicateTest {

  private static final int COLUMN = 3;

  /** The literal is id 7 in segment 0, id 900 in segment 1, and absent from segment 2. */
  private static long[] literalCells() {
    return new long[] {ProjectionIndexRowGroupPage.packSegmentCell(0, 7),
        ProjectionIndexRowGroupPage.packSegmentCell(1, 900), ColumnPredicate.SEGMENT_LITERAL_ABSENT};
  }

  @Test
  @DisplayName("a leaf picks the literal of ITS segment, from any cell it holds")
  void aLeafPicksItsOwnSegmentsLiteral() {
    final ColumnPredicate p = ColumnPredicate.segmentScopedEquality(COLUMN, Op.EQ, literalCells());
    assertEquals(ProjectionIndexRowGroupPage.packSegmentCell(0, 7),
        p.literalForLeaf(ProjectionIndexRowGroupPage.packSegmentCell(0, 12345)));
    assertEquals(ProjectionIndexRowGroupPage.packSegmentCell(1, 900),
        p.literalForLeaf(ProjectionIndexRowGroupPage.packSegmentCell(1, 4)),
        "the same literal has a different id in segment 1, and the leaf must use THAT one");
    assertEquals(ColumnPredicate.SEGMENT_LITERAL_ABSENT,
        p.literalForLeaf(ProjectionIndexRowGroupPage.packSegmentCell(2, 5)));
  }

  @Test
  @DisplayName("a segment beyond the table reads as absent, never as segment 0's literal")
  void anUnknownSegmentIsAbsentNotSegmentZero() {
    final ColumnPredicate p = ColumnPredicate.segmentScopedEquality(COLUMN, Op.EQ, literalCells());
    assertEquals(ColumnPredicate.SEGMENT_LITERAL_ABSENT,
        p.literalForLeaf(ProjectionIndexRowGroupPage.packSegmentCell(9, 1)),
        "falling back to entry 0 would compare against another segment's id space");
  }

  @Test
  @DisplayName("the zone map prunes on the leaf's own literal, not on longLit")
  void zonePruningUsesThePerLeafLiteral() {
    final ColumnPredicate eq = ColumnPredicate.segmentScopedEquality(COLUMN, Op.EQ, literalCells());
    // A leaf of segment 1 holding ids 100..1000: the literal (id 900) is inside, so it must be kept.
    final long lo = ProjectionIndexRowGroupPage.packSegmentCell(1, 100);
    final long hi = ProjectionIndexRowGroupPage.packSegmentCell(1, 1000);
    assertFalse(ProjectionIndexByteScan.zoneSkip(eq, lo, hi), "the leaf's zone contains the literal");

    // A leaf of segment 1 holding ids 1..50: the literal cannot be there.
    assertTrue(ProjectionIndexByteScan.zoneSkip(eq, ProjectionIndexRowGroupPage.packSegmentCell(1, 1),
        ProjectionIndexRowGroupPage.packSegmentCell(1, 50)), "the literal is above this leaf's ids");

    // Segment 2 lacks the value entirely: EQ can match nothing, NE matches every present row.
    final long s2lo = ProjectionIndexRowGroupPage.packSegmentCell(2, 1);
    final long s2hi = ProjectionIndexRowGroupPage.packSegmentCell(2, 99);
    assertTrue(ProjectionIndexByteScan.zoneSkip(eq, s2lo, s2hi), "EQ against a value the segment lacks");
    final ColumnPredicate ne = ColumnPredicate.segmentScopedEquality(COLUMN, Op.NE, literalCells());
    assertFalse(ProjectionIndexByteScan.zoneSkip(ne, s2lo, s2hi), "NE against a value the segment lacks matches");
  }

  @Test
  @DisplayName("an id-space predicate over cells would prune the wrong leaves — the bug the form prevents")
  void aPlainLiteralWouldPruneWrongly() {
    // What a naive `numeric(column, EQ, id)` does to a segment-1 leaf: the id (7) sits far below
    // every cell in that leaf, so the whole leaf is pruned though it may hold the value under id 900.
    final ColumnPredicate naive = ColumnPredicate.numeric(COLUMN, Op.EQ, 7L);
    assertTrue(ProjectionIndexByteScan.zoneSkip(naive, ProjectionIndexRowGroupPage.packSegmentCell(1, 100),
        ProjectionIndexRowGroupPage.packSegmentCell(1, 1000)), "a bare id prunes a segment-1 leaf outright");
  }

  @Test
  @DisplayName("only EQ and NE are questions about one id")
  void otherOpsAreRefused() {
    assertThrows(IllegalArgumentException.class,
        () -> ColumnPredicate.segmentScopedEquality(COLUMN, Op.STR_CONTAINS, literalCells()));
    assertThrows(IllegalArgumentException.class, () -> ColumnPredicate.segmentScopedEquality(COLUMN, Op.EQ, null));
  }
}
