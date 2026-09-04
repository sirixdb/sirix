/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Index-type-specific tombstone semantics for HOT leaf deletion and physical compaction. */
final class HOTLeafPageTombstoneSemanticsTest {

  private static final byte[] KEY_FE = {1};
  private static final byte[] KEY_TOMBSTONE = {2};
  private static final byte[] KEY_LIVE = {3};
  private static final byte[] FE = {(byte) 0xFE};

  @Test
  void projectionDeleteUsesZeroLengthAndAcceptsAnExistingZeroLengthTombstone() {
    final HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION);
    try {
      assertTrue(leaf.put(KEY_FE, FE));
      assertTrue(leaf.put(KEY_TOMBSTONE, new byte[0]));

      assertTrue(leaf.delete(KEY_FE), "live opaque 0xFE must not be mistaken for a posting tombstone");
      assertArrayEquals(new byte[0], leaf.copyStoredValue(leaf.findEntry(KEY_FE)));
      assertFalse(leaf.delete(KEY_FE), "a projection tombstone must be idempotent");
      assertFalse(leaf.delete(KEY_TOMBSTONE), "an existing zero-length tombstone must not throw or rewrite");
    } finally {
      leaf.close();
    }
  }

  @Test
  void slidingCarryUsesEachIndexTypesOwnTombstoneWireContract() {
    final HOTLeafPage postingOldest = new HOTLeafPage(4, 1, IndexType.PATH);
    final HOTLeafPage projectionOldest = new HOTLeafPage(5, 1, IndexType.PROJECTION);
    HOTLeafPage postingModified = null;
    HOTLeafPage projectionModified = null;
    try {
      assertTrue(postingOldest.put(KEY_LIVE, new byte[0]));
      assertTrue(projectionOldest.put(KEY_LIVE, new byte[0]));
      postingModified = postingOldest.copy();
      projectionModified = projectionOldest.copy();
      postingModified.clearDirtyBitmap();
      projectionModified.clearDirtyBitmap();

      VersioningType.carryForwardAgingHOTEntries(List.of(postingOldest), postingModified);
      VersioningType.carryForwardAgingHOTEntries(List.of(projectionOldest), projectionModified);

      assertTrue(postingModified.isEntryDirty(0),
          "posting indexes must classify tombstones only through the serializer wire marker");
      assertFalse(projectionModified.isEntryDirty(0),
          "a zero-length projection value is its tombstone and must not be carried forward");
    } finally {
      if (postingModified != null) {
        postingModified.close();
      }
      if (projectionModified != null) {
        projectionModified.close();
      }
      postingOldest.close();
      projectionOldest.close();
    }
  }

  @Test
  void fragmentMergeFailsClosedOnAnUnreadableStoredValue() {
    final HOTLeafPage newest = new HOTLeafPage(4, 2, IndexType.PROJECTION);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment corruptSlot = arena.allocate(8);
      corruptSlot.set(ValueLayout.JAVA_SHORT_UNALIGNED, 0, (short) 0);
      corruptSlot.set(ValueLayout.JAVA_SHORT_UNALIGNED, 2, (short) 100);
      final HOTLeafPage corruptOlder =
          new HOTLeafPage(4, 1, IndexType.PROJECTION, corruptSlot, null, new int[] {0}, 1, 4, new byte[0], 0);
      try {
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> VersioningType.SLIDING_SNAPSHOT.combineHOTLeafPages(List.of(newest, corruptOlder), 3,
                mock(StorageEngineReader.class)));
        assertTrue(failure.getMessage().contains("unreadable value"));
      } finally {
        corruptOlder.close();
      }
    } finally {
      newest.close();
    }
  }
}
