/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** A read optimization must retain the original packed layout and dirty-entry ownership. */
final class HOTProjectionMergeBytesTest {
  @Test
  void reconstructionMatchesOrdinaryInsertionByteForByte() {
    final Random random = new Random(0x514A7);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    for (int trial = 0; trial < 40; trial++) {
      final List<HOTLeafPage> fragments = new ArrayList<>(32);
      HOTLeafPage expected = null;
      HOTLeafPage actual = null;
      try {
        for (int fragment = 0; fragment < 32; fragment++) {
          // Both native images and the compact decoder's independently owned heap slots can be
          // reconstruction sources. The result must have the same packed bytes for either one.
          final HOTLeafPage page = (fragment & 1) == 0
              ? new HOTLeafPage(71, 32 - fragment, IndexType.PROJECTION)
              : new HOTLeafPage(71, 32 - fragment, IndexType.PROJECTION,
                  MemorySegment.ofArray(new byte[HOTLeafPage.DEFAULT_SIZE]), null, new int[HOTLeafPage.MAX_ENTRIES], 0,
                  0);
          fragments.add(page);
          for (int entry = 0; entry < 7; entry++) {
            final int id = random.nextInt(160);
            final byte[] key = {(byte) (id >>> 5), 12, 23, (byte) id};
            final byte[] value = new byte[random.nextInt(97)];
            random.nextBytes(value);
            assertTrue(page.putOrReplace(key, value));
          }
          if (fragment == 8 + trial % 24) {
            page.setCompleteDump(true);
          }
        }
        expected = fragments.getFirst().copy();
        expected.setCompletePageRef(null);
        expected.clearDirtyBitmap();
        for (int fragment = 1; fragment < fragments.size(); fragment++) {
          final HOTLeafPage older = fragments.get(fragment);
          for (int row = 0; row < older.getEntryCount(); row++) {
            final byte[] key = older.getKey(row);
            if (expected.findEntry(key) < 0) {
              assertTrue(expected.putOrReplace(key, older.copyStoredValue(row)));
            }
          }
          if (older.isCompleteDump()) {
            break;
          }
        }
        expected.recomputePrefixForCombine();
        actual = VersioningType.SLIDING_SNAPSHOT.combineHOTLeafPages(fragments, 32, reader);
        assertEquals(expected.getEntryCount(), actual.getEntryCount());
        assertArrayEquals(expected.getCommonPrefix(), actual.getCommonPrefix());
        assertEquals(expected.getUsedSlotsSize(), actual.getUsedSlotsSize());
        assertArrayEquals(expected.slots().asSlice(0, expected.getUsedSlotsSize()).toArray(ValueLayout.JAVA_BYTE),
            actual.slots().asSlice(0, actual.getUsedSlotsSize()).toArray(ValueLayout.JAVA_BYTE));
        for (int row = 0; row < expected.getEntryCount(); row++) {
          assertEquals(expected.getSlotOffset(row), actual.getSlotOffset(row));
          assertEquals(expected.isEntryDirty(row), actual.isEntryDirty(row));
          assertArrayEquals(expected.getKey(row), actual.getKey(row));
          assertArrayEquals(expected.copyStoredValue(row), actual.copyStoredValue(row));
        }
      } finally {
        if (expected != null) {
          expected.close();
        }
        if (actual != null) {
          actual.close();
        }
        for (final HOTLeafPage fragment : fragments) {
          fragment.close();
        }
      }
    }
  }
}
