/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.HOTLeafPage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSlotLayoutTest {
  @ParameterizedTest
  @EnumSource(ProjectionSlotLayout.class)
  void allKindsAndBoundaryIdsRetainTheirExactOwner(final ProjectionSlotLayout layout) {
    for (final long row : new long[] {1, 2, (1 << 24) - 1, 1 << 24}) {
      for (int kind = 0; kind <= 0xFFFF; kind++) {
        final long key = layout.slotKey(row, kind);
        assertEquals(row, layout.rowGroupId(key));
        assertEquals(kind, layout.slotKind(key));
        assertEquals(key, HOTLeafPage.overflowPageRefOwnerSlot(HOTLeafPage.overflowPageRefKey(key, 0)));
        assertTrue(key < ProjectionIndexFences.CHUNK_SLOT_BASE);
      }
    }
    assertThrows(IllegalArgumentException.class, () -> layout.slotKey(0, 0));
    assertThrows(IllegalArgumentException.class, () -> layout.slotKey((1 << 24) + 1L, 0));
    assertThrows(IllegalArgumentException.class, () -> layout.slotKey(1, -1));
    assertThrows(IllegalArgumentException.class, () -> layout.slotKey(1, 0x10000));
    assertThrows(IllegalArgumentException.class, () -> layout.segmentSlot(1, 0xFFFF));
    assertFalse(layout.contains(-1));
    assertFalse(layout.contains(0));
    assertFalse(layout.contains(ProjectionIndexFences.CHUNK_SLOT_BASE));
  }

  @Test
  void columnRangesAreDisjointFromOldKeysAndEachOther() {
    final ProjectionSlotLayout layout = ProjectionSlotLayout.COLUMN_MAJOR;
    final long oldMaximum = ProjectionSlotLayout.ROW_GROUP_MAJOR.slotKey(1 << 24, 0xFFFF);
    assertTrue(oldMaximum < layout.descriptorSlot(1));
    for (int kind = 0; kind < 0xFFFF; kind++) {
      assertTrue(layout.slotKey(1 << 24, kind) < layout.slotKey(1, kind + 1));
    }
    assertThrows(IllegalArgumentException.class, () -> layout.rowGroupId(layout.slotKey(1, 5) - 1));
    assertThrows(IllegalArgumentException.class, () -> layout.rowGroupId(layout.slotKey(1 << 24, 5) + 1));
    assertThrows(IllegalArgumentException.class, () -> layout.rowGroupId(oldMaximum));
  }

  @ParameterizedTest
  @EnumSource(ProjectionSlotLayout.class)
  void metadataAndStaleMarkersPreserveTheRevisionLayout(final ProjectionSlotLayout layout) {
    final ProjectionIndexMetadata original =
        new ProjectionIndexMetadata("/[]", new String[] {"/[]/n"}, new String[] {"n"},
            new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 5, 7).withSlotLayout(layout);
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(original.serialize());
    assertNotNull(parsed);
    assertEquals(layout, parsed.slotLayout());
    assertEquals(layout.metadataVersion(), original.serialize()[4]);
    assertArrayEquals(original.serialize(), parsed.serialize());
    final ProjectionIndexMetadata stale = ProjectionIndexMetadata.parse(
        ProjectionIndexMetadata.staleTombstone(ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED)
                               .withSlotLayout(layout)
                               .serialize());
    assertNotNull(stale);
    assertTrue(stale.isStale());
    assertEquals(layout, stale.slotLayout());
    assertEquals(ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED, stale.staleReason());
  }
}
