/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HOTLeafFullImagePackingTest {
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void completeImageContainsOnlyLiveRecordsWithoutMutatingSource(final VersioningType type) throws IOException {
    for (final int window : new int[] {3, 32}) {
      final ResourceConfiguration config = ResourceConfiguration.newBuilder("packing")
                                                                .versioningApproach(type)
                                                                .maxNumberOfRevisionsToRestore(window)
                                                                .build();
      try (HOTLeafPage leaf = new HOTLeafPage(7, 1, IndexType.PROJECTION)) {
        for (int i = 0; i < 4; i++) {
          assertTrue(leaf.put(new byte[] {(byte) i}, new byte[16]));
        }
        assertTrue(leaf.updateValue(0, new byte[64]));
        assertTrue(leaf.updateValue(1, new byte[3]));
        assertTrue(leaf.deleteAt(2));
        leaf.setCompleteDump(true);
        final byte[] before = leaf.slots().asSlice(0, leaf.getUsedSlotsSize()).toArray(ValueLayout.JAVA_BYTE);
        final int[] offsets = new int[leaf.getEntryCount()];
        int liveSize = 0;
        for (int i = 0; i < offsets.length; i++) {
          offsets[i] = leaf.getSlotOffset(i);
          liveSize += leaf.getSlotAsByteArray(i).length;
        }
        assertTrue(before.length > liveSize, "fixture must leave retired growth/shrink/delete records");
        try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1 << 16)) {
          new PagePersister().serializePage(config, sink, leaf, SerializationType.DATA);
          final byte[] wire = sink.toByteArray();
          // kind/version/flags + varlong page key + revision + index kind + prefix length + counts
          assertEquals(19 + leaf.getCommonPrefixLen() + 4 * offsets.length + liveSize, wire.length);
          try (HOTLeafPage decoded = (HOTLeafPage) new PagePersister().deserializePage(config, Bytes.wrapForRead(wire),
              SerializationType.DATA)) {
            assertEquals(liveSize, decoded.getUsedSlotsSize());
            assertTrue(decoded.isCompleteDump());
            int offset = 0;
            for (int i = 0; i < offsets.length; i++) {
              assertEquals(offset, decoded.getSlotOffset(i));
              assertArrayEquals(leaf.getKey(i), decoded.getKey(i));
              assertArrayEquals(leaf.copyStoredValue(i), decoded.copyStoredValue(i));
              offset += decoded.getSlotAsByteArray(i).length;
            }
          }
          assertArrayEquals(before, leaf.slots().asSlice(0, leaf.getUsedSlotsSize()).toArray(ValueLayout.JAVA_BYTE));
          for (int i = 0; i < offsets.length; i++) {
            assertEquals(offsets[i], leaf.getSlotOffset(i));
            assertTrue(leaf.isEntryDirty(i));
          }
        }

      }
      try (HOTLeafPage empty = new HOTLeafPage(7, 1, IndexType.PROJECTION);
          MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1 << 16)) {
        new PagePersister().serializePage(config, sink, empty, SerializationType.DATA);
        try (HOTLeafPage decoded = (HOTLeafPage) new PagePersister().deserializePage(config,
            Bytes.wrapForRead(sink.toByteArray()), SerializationType.DATA)) {
          assertEquals(0, decoded.getEntryCount());
          assertEquals(0, decoded.getUsedSlotsSize());
        }
      }

    }
  }
}
