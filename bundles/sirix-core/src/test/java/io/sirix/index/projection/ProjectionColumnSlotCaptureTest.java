/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class ProjectionColumnSlotCaptureTest {
  private static final int BODY = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
  private static final long FIRST = ProjectionSlotLayout.COLUMN_MAJOR.segmentSlot(1, BODY);
  private static final long SECOND = ProjectionSlotLayout.COLUMN_MAJOR.segmentSlot(2, BODY);

  @Test
  void aChainRangeSkipsOtherColumnsAndRetainsCallerOrder() {
    try (Fixture fixture = fixture(new byte[] {0, 42, 43}, false)) {
      final byte[][] out = new byte[2][];
      ProjectionIndexHOTStorage.readColumnSlotRange(fixture.reader, 0, new long[] {SECOND, FIRST}, 0, 2, out);
      assertArrayEquals(new byte[] {91}, out[0]);
      assertArrayEquals(new byte[] {42, 43}, out[1]);
      // One decode to identify each requested key, one for its captured value. The fixture also
      // contains an invalid segment in another column; that column's bytes are never inspected.
      verify(fixture.leaf, times(4)).decodeKey8BE(anyInt());
    }
  }

  @Test
  void anEvictionAfterCopyDiscardsTheSpeculativePayloadAndReloads() {
    try (Fixture fixture = fixture(new byte[] {0, 42, 43}, false); HOTLeafPage replacement = fixture.replacement()) {
      final AtomicInteger evictions = new AtomicInteger();
      when(fixture.reader.loadHOTPageAndGuard(fixture.root)).thenAnswer(invocation -> {
        assertTrue(replacement.acquireGuard());
        return replacement;
      });
      doAnswer(invocation -> {
        invocation.callRealMethod();
        final byte[] target = invocation.getArgument(2);
        final int offset = invocation.getArgument(3);
        target[offset] ^= 0x7F;
        fixture.leaf.close();
        fixture.root.setPage(null);
        evictions.incrementAndGet();
        return null;
      }).when(fixture.leaf).copyRefInto(anyLong(), eq(1), any(byte[].class), anyInt(), anyInt());
      final byte[][] out = new byte[2][];
      ProjectionIndexHOTStorage.readColumnSlotRange(fixture.reader, 0, new long[] {FIRST, SECOND}, 0, 2, out);
      assertEquals(1, evictions.get());
      assertArrayEquals(new byte[] {42, 43}, out[0]);
      assertArrayEquals(new byte[] {91}, out[1]);
      verify(fixture.reader, atLeastOnce()).loadHOTPageAndGuard(fixture.root);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  void stableMalformedValuesAndMissingReferencesFailClosed(final int malformed) {
    final byte[] value = switch (malformed) {
      case 0 -> new byte[] {2};
      case 1 -> new byte[] {1};
      case 2 -> new byte[] {1, 5};
      default -> new byte[] {0, 42};
    };
    try (Fixture fixture = fixture(value, malformed == 3)) {
      assertThrows(IllegalStateException.class, () -> ProjectionIndexHOTStorage.readColumnSlotRange(fixture.reader, 0,
          new long[] {FIRST}, 0, 1, new byte[1][]));
    }
  }

  private static Fixture fixture(final byte[] firstValue, final boolean unreadable) {
    final ProjectionIndexMetadata metadata =
        new ProjectionIndexMetadata("/[]", new String[] {"/[]/a", "/[]/b"}, new String[] {"a", "b"},
            new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
                ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG},
            2, 1).withSlotLayout(ProjectionSlotLayout.COLUMN_MAJOR);
    final TreeMap<Long, byte[]> slots = new TreeMap<>();
    slots.put(0L, blob(metadata.serialize()));
    slots.put(FIRST, firstValue);
    slots.put(SECOND, new byte[] {0, 91});
    slots.put(
        ProjectionSlotLayout.COLUMN_MAJOR.segmentSlot(1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(1)),
        new byte[] {2});
    int bytes = 0;
    for (final byte[] value : slots.values()) {
      bytes += 12 + value.length;
    }
    final byte[] packed = new byte[bytes];
    final int[] offsets = new int[slots.size()];
    int index = 0;
    int offset = 0;
    for (final Map.Entry<Long, byte[]> entry : slots.entrySet()) {
      offsets[index++] = offset;
      packed[offset] = 8;
      PathKeySerializer.INSTANCE.serialize(entry.getKey(), packed, offset + 2);
      final int declared = unreadable && entry.getKey() == FIRST
          ? bytes
          : entry.getValue().length;
      packed[offset + 10] = (byte) declared;
      packed[offset + 11] = (byte) (declared >>> 8);
      System.arraycopy(entry.getValue(), 0, packed, offset + 12, entry.getValue().length);
      offset += 12 + entry.getValue().length;
    }
    final HOTLeafPage leaf = spy(new HOTLeafPage(1, 1, IndexType.PROJECTION, MemorySegment.ofArray(packed), null,
        offsets, offsets.length, packed.length, new byte[0], 0));
    final ProjectionIndexPage projectionPage = new ProjectionIndexPage();
    final PageReference root = projectionPage.getOrCreateReference(0);
    root.setKey(1234L);
    root.setPage(leaf);
    final RevisionRootPage revision = mock(RevisionRootPage.class);
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    when(reader.getActualRevisionRootPage()).thenReturn(revision);
    when(reader.getProjectionIndexPage(revision)).thenReturn(projectionPage);
    return new Fixture(leaf, reader, root, packed, offsets);
  }

  private static byte[] blob(final byte[] payload) {
    final byte[] result = new byte[17 + payload.length];
    RowGroupDescriptor.putIntLE(result, 0, 0x42584950);
    RowGroupDescriptor.putIntLE(result, 5, Integer.MIN_VALUE | payload.length);
    RowGroupDescriptor.putLongLE(result, 9, ProjectionIndexColumnSegmentCodec.contentHash(payload));
    System.arraycopy(payload, 0, result, 17, payload.length);
    return result;
  }

  private record Fixture(HOTLeafPage leaf, StorageEngineReader reader, PageReference root, byte[] packed,
      int[] offsets) implements AutoCloseable {
    HOTLeafPage replacement() {
      return new HOTLeafPage(1, 1, IndexType.PROJECTION, MemorySegment.ofArray(packed.clone()), null, offsets.clone(),
          offsets.length, packed.length, new byte[0], 0);
    }

    @Override
    public void close() {
      leaf.close();
    }
  }
}
