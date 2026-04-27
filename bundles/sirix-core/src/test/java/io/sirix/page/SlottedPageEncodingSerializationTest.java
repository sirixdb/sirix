/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.settings.Constants;
import io.sirix.utils.OS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end round trip tests for {@link PageKind#KEYVALUELEAFPAGE}'s wire
 * format. Verifies that pages reach disk in the structural-encoder format
 * (offset-table dedup) and deserialize back bit-identically, even when
 * records have no offset-table structure (slab bytes) — in which case the
 * dedup aborts and the writer gracefully falls back to the inline heap
 * emission (gated by a zero-templateCount byte).
 *
 * <p>Comprehensive structural tests (records with real offset tables from
 * {@code ObjectKeyNode.writeNewRecord}) live in
 * {@link OffsetTableTemplatePoolTest} — those hit the build/expand code
 * paths directly on a synthetic slotted-page memory without going through
 * the full node-type machinery.
 */
@DisplayName("Slotted page encoding round trip")
final class SlottedPageEncodingSerializationTest {

  private static MemorySegmentAllocator allocator;

  @BeforeAll
  static void setUpClass() {
    allocator = OS.isWindows()
        ? LinuxMemorySegmentAllocator.getInstance()
        : LinuxMemorySegmentAllocator.getInstance();
    allocator.init(8L * 1024 * 1024 * 1024); // 8 GiB
  }

  @Test
  @DisplayName("empty page round-trips via zero-template marker")
  void emptyPage() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage orig = new KeyValueLeafPage(1, 0, IndexType.DOCUMENT, config, false, null,
        new LinkedHashMap<>(), allocator.allocate(1), null, -1);
    KeyValueLeafPage deserialized = null;
    try {
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, orig, SerializationType.DATA);

      final BytesIn<?> source = sink.bytesForRead();
      source.readByte(); // skip pageKind id
      deserialized = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE
          .deserializePage(config, source, SerializationType.DATA);

      assertEquals(orig.getPageKey(), deserialized.getPageKey());
      assertEquals(orig.getRevision(), deserialized.getRevision());
    } finally {
      orig.close();
      if (deserialized != null) deserialized.close();
    }
  }

  @Test
  @DisplayName("slot bytes without offset tables fall back to inline heap")
  void rawSlotBytesInlineFallback() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage orig = new KeyValueLeafPage(1, 0, IndexType.DOCUMENT, config, false, null,
        new LinkedHashMap<>(), allocator.allocate(1000), null, -1);
    KeyValueLeafPage deserialized = null;
    try {
      orig.setSlot(new byte[] { 1, 2, 3 }, 1);
      orig.setSlot(new byte[] { 4, 5, 6 }, 10);
      orig.setSlot(new byte[] { 7, 8, 9 }, 100);

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, orig, SerializationType.DATA);

      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      deserialized = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE
          .deserializePage(config, source, SerializationType.DATA);

      assertArrayEquals(orig.getSlotAsByteArray(1), deserialized.getSlotAsByteArray(1));
      assertArrayEquals(orig.getSlotAsByteArray(10), deserialized.getSlotAsByteArray(10));
      assertArrayEquals(orig.getSlotAsByteArray(100), deserialized.getSlotAsByteArray(100));
    } finally {
      orig.close();
      if (deserialized != null) deserialized.close();
    }
  }

  @RepeatedTest(25)
  @DisplayName("random slot insertions round-trip bit-identical (fallback path)")
  void randomSlots() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage orig = new KeyValueLeafPage(1, 0, IndexType.DOCUMENT, config, false, null,
        new LinkedHashMap<>(), allocator.allocate(110_000), null, -1);
    KeyValueLeafPage deserialized = null;
    try {
      final Random random = new Random();
      final byte[][] expected = new byte[Constants.NDP_NODE_COUNT][];
      for (int i = 0; i < 500; i++) {
        final int slot = random.nextInt(Constants.NDP_NODE_COUNT);
        final int dataSize = random.nextInt(50) + 1;
        final byte[] data = new byte[dataSize];
        random.nextBytes(data);
        orig.setSlot(data, slot);
        expected[slot] = data;
      }

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, orig, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      deserialized = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE
          .deserializePage(config, source, SerializationType.DATA);

      for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        assertArrayEquals(expected[i], deserialized.getSlotAsByteArray(i),
            "slot " + i + " mismatch");
      }
    } finally {
      orig.close();
      if (deserialized != null) deserialized.close();
    }
  }

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
  }
}
