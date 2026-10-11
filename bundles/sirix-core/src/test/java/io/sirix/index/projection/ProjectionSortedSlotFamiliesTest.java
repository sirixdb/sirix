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
 * The sorted-view slot families above {@link ProjectionSortedLeafStore#LEAF_SLOT_BASE} are
 * {@code 1 << 32} apart and pairwise disjoint; the separator family must not alias the
 * numeric-proof family that already occupied selector 4 when separators were introduced.
 */
final class ProjectionSortedSlotFamiliesTest {

  private static long family(final int selector) {
    return ProjectionSortedLeafStore.LEAF_SLOT_BASE + ((long) selector << 32);
  }

  @Test
  @DisplayName("every sorted-view family owns its own selector")
  void sortedViewFamiliesAreDisjoint() {
    assertEquals(family(0), ProjectionSortedLeafStore.LEAF_SLOT_BASE);
    assertEquals(family(1), ProjectionSortedDirectory.HEADER_SLOT);
    assertEquals(family(2), ProjectionSortedGroupSummary.SLOT_BASE);
    assertEquals(family(3), ProjectionSortedLeafBounds.SLOT_BASE);
    assertEquals(family(4), ProjectionNumericProofs.SLOT_BASE);
    assertEquals(family(5), ProjectionSortedGroupSma.HEADER_SLOT);
    assertEquals(family(6), ProjectionSortedDirectoryNode.SEPARATOR_SLOT_BASE);
    assertTrue(ProjectionNumericProofs.HEADER_SLOT < ProjectionSortedGroupSma.HEADER_SLOT,
        "the numeric-proof header is the last slot of its own family");
    assertTrue(
        ProjectionSortedDirectoryNode.SEPARATOR_SLOT_BASE + (1L << 32) <= ProjectionStructuralOrderDirectory.BASE,
        "the separator family ends below the structural-order directory");
  }

  @Test
  @DisplayName("a separator id never aliases a column-0 numeric-proof chunk")
  void separatorIdsDoNotAliasNumericProofs() {
    for (int id = 1; id <= 1_024; id++) {
      assertNotEquals(ProjectionNumericProofs.slot(0, id), ProjectionSortedDirectoryNode.SEPARATOR_SLOT_BASE + id);
    }
    assertTrue(ProjectionNumericProofs.slot(0, 1) < ProjectionSortedGroupSma.HEADER_SLOT);
  }
}
