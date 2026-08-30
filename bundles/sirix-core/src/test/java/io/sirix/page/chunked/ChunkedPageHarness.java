/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.Utils;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageKind;
import io.sirix.page.PageLayout;
import io.sirix.page.SerializationType;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Shared machinery for the chunked-body tests: write a page both ways, read it back, compare what
 * came back, and read the wire layout the way a reader reads it.
 */
final class ChunkedPageHarness {

  private ChunkedPageHarness() {}

  /**
   * Serialize a page and hand back exactly the bytes that would reach disk. The sticky codec election
   * is reset first, so the codec depends on the page's content alone rather than on what this thread
   * serialized before it.
   */
  static MemorySegment serialize(final ResourceConfiguration config, final KeyValueLeafPage page,
      final boolean chunked) {
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunked);
    try {
      PageKind.resetStickyCodecElectionForCurrentThread();
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      final long length = sink.writePosition();
      final BytesIn<?> read = sink.bytesForRead();
      final MemorySegment source = ((MemorySegmentBytesIn) read).getSource();
      final MemorySegment copy = Arena.ofAuto().allocate(length);
      MemorySegment.copy(source, 0, copy, 0, length);
      return copy;
    } finally {
      ChunkedBodyConfig.setEnabledForTesting(previous);
    }
  }

  static KeyValueLeafPage deserialize(final ResourceConfiguration config, final MemorySegment wire) {
    final MemorySegmentBytesIn in = new MemorySegmentBytesIn(wire);
    in.readByte(); // page-kind id, consumed by the caller in production too
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, in, SerializationType.DATA);
  }

  /**
   * Decode a page without expanding its records, the way a point lookup would ask for it. On a
   * monolith page this is the eager decode — the caller states a preference, not a requirement.
   */
  static KeyValueLeafPage deserializeLazily(final ResourceConfiguration config, final MemorySegment wire) {
    final MemorySegmentBytesIn in = new MemorySegmentBytesIn(wire);
    in.readByte();
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePageLazily(config, in, SerializationType.DATA, null);
  }

  /** Build a page to a recipe, serialize it chunked, and throw the page away. */
  static MemorySegment serializeChunked(final ResourceConfiguration config, final ChunkedPageGenerator.Recipe recipe) {
    final KeyValueLeafPage page = ChunkedPageGenerator.build(recipe, config);
    try {
      return serialize(config, page, true);
    } finally {
      page.close();
    }
  }

  /** The slots a decoded page has records in, in ascending order. */
  static int[] populatedSlots(final KeyValueLeafPage page) {
    final MemorySegment segment = page.getSlottedPage();
    final int[] slots = new int[PageLayout.getPopulatedCount(segment)];
    int at = 0;
    for (int slot = 0; slot < PageLayout.SLOT_COUNT; slot++) {
      if (PageLayout.isSlotPopulated(segment, slot)) {
        slots[at++] = slot;
      }
    }
    return slots;
  }

  /**
   * The in-memory heap range each chunk's records occupy, as {@code [from, to)} offsets from the
   * heap's start. Derived from the decoded directory and the chunk table's entry ranges, so it is the
   * test's own arithmetic rather than the reader's.
   */
  static int[][] chunkHeapRanges(final KeyValueLeafPage page, final ChunkedLayout layout) {
    final MemorySegment segment = page.getSlottedPage();
    final int[] slots = populatedSlots(page);
    final int[][] ranges = new int[layout.chunkCount][2];
    for (int c = 0; c < layout.chunkCount; c++) {
      final int firstSlot = slots[layout.chunkFirstEntry[c]];
      final int lastSlot = slots[layout.chunkFirstEntry[c] + layout.chunkEntryCount[c] - 1];
      ranges[c][0] = PageLayout.getDirHeapOffset(segment, firstSlot);
      ranges[c][1] = PageLayout.getDirHeapOffset(segment, lastSlot) + PageLayout.getDirDataLength(segment, lastSlot);
    }
    return ranges;
  }

  /** Whether every byte of a heap range is the poison an unexpanded chunk is filled with. */
  static boolean isAllPoison(final KeyValueLeafPage page, final int[] range) {
    final MemorySegment segment = page.getSlottedPage();
    for (int off = range[0]; off < range[1]; off++) {
      if (segment.get(ValueLayout.JAVA_BYTE, PageLayout.HEAP_START + off) != ChunkedBodyConfig.POISON_BYTE) {
        return false;
      }
    }
    return range[1] > range[0];
  }

  /**
   * Compare two decoded pages as pages, not as slot lists: the on-disk header and slot bitmap, the
   * directory entry of every populated slot, and the whole record heap — DeweyID trailers included,
   * since they live inside the heap rather than in any slot's bytes. A slot-by-slot comparison alone
   * would pass a page whose trailers or heap layout had shifted.
   */
  static void assertSameSlottedPage(final KeyValueLeafPage expected, final KeyValueLeafPage actual, final String what) {
    final MemorySegment a = expected.getSlottedPage();
    final MemorySegment b = actual.getSlottedPage();
    assertArrayEquals(bytes(a, 0, PageLayout.DISK_HEADER_BITMAP_SIZE), bytes(b, 0, PageLayout.DISK_HEADER_BITMAP_SIZE),
        what + ": header + slot bitmap");
    final int heapEnd = PageLayout.getHeapEnd(a);
    assertEquals(heapEnd, PageLayout.getHeapEnd(b), what + ": heap size");
    assertArrayEquals(bytes(a, PageLayout.HEAP_START, heapEnd), bytes(b, PageLayout.HEAP_START, heapEnd),
        what + ": record heap");
    for (int slot = 0; slot < PageLayout.SLOT_COUNT; slot++) {
      if (!PageLayout.isSlotPopulated(a, slot)) {
        continue;
      }
      assertEquals(PageLayout.getDirDataLength(a, slot), PageLayout.getDirDataLength(b, slot),
          what + ": slot " + slot + " directory length");
      assertEquals(PageLayout.getDirNodeKindId(a, slot), PageLayout.getDirNodeKindId(b, slot),
          what + ": slot " + slot + " directory kind");
      assertEquals(PageLayout.getDirHeapOffset(a, slot), PageLayout.getDirHeapOffset(b, slot),
          what + ": slot " + slot + " directory heap offset");
      assertArrayEquals(expected.getSlotAsByteArray(slot), actual.getSlotAsByteArray(slot),
          what + ": slot " + slot + " bytes");
    }
  }

  static byte[] bytes(final MemorySegment segment, final long offset, final int length) {
    return segment.asSlice(offset, length).toArray(ValueLayout.JAVA_BYTE);
  }

  static byte[] toArray(final MemorySegment wire) {
    return wire.toArray(ValueLayout.JAVA_BYTE);
  }

  /**
   * Where the parts of a chunked body sit, and what its prefix says, parsed as a reader parses it.
   */
  static final class ChunkedLayout {
    int populatedCount;
    int onDiskHeapSize;
    int templateCount;
    /** {@code -1} when the body is degenerate and carries no structural-flags byte. */
    int structuralFlags = -1;
    /**
     * The second structural-flags byte, or {@code 0} when the first one does not announce it. Written
     * only for the levers that did not fit the first byte, so a page using none of them has none.
     */
    int extendedStructuralFlags;
    long fsstDictId;
    int bodyTotalLen;
    int metaRawLen;
    int metaEncLen;
    int metaCodec;
    int chunkCount;
    long chunkTableOffset;
    long metaPayloadOffset;
    long regionTableOffset;
    int[] chunkFirstEntry = new int[0];
    int[] chunkEntryCount = new int[0];
    int[] chunkRawLen = new int[0];
    int[] chunkEncLen = new int[0];
    int[] chunkCodec = new int[0];
    long[] chunkRowOffset = new long[0];
    long[] chunkPayloadOffset = new long[0];
  }

  /**
   * Parse a chunked page's envelope, prefix and chunk table. Deliberately a second implementation of
   * the reader's own walk: a test that reused the reader's parser could not catch the reader and
   * writer drifting together.
   */
  static ChunkedLayout parseChunkedLayout(final MemorySegment wire) {
    final ChunkedLayout layout = new ChunkedLayout();
    final MemorySegmentBytesIn in = new MemorySegmentBytesIn(wire);
    in.readByte(); // page-kind id
    in.readByte(); // binary version
    assertEquals(ChunkedBodyConfig.FLAG_CHUNKED_BODY, in.readByte(), "the envelope must flag a chunked body");
    Utils.getVarLong(in); // record page key
    in.readInt(); // revision
    in.readByte(); // index type
    in.skip(PageLayout.DISK_HEADER_BITMAP_SIZE);
    layout.populatedCount = in.readInt();
    layout.onDiskHeapSize = in.readInt();
    layout.templateCount = in.readByte() & 0xFF;
    if (layout.templateCount > 0) {
      layout.structuralFlags = in.readByte() & 0xFF;
      // Bit 7 announces a second flags byte. Spelled out rather than imported: this parse is a second
      // implementation of the reader's walk on purpose, and sharing the constant would let the two
      // drift together.
      if ((layout.structuralFlags & 0x80) != 0) {
        layout.extendedStructuralFlags = in.readByte() & 0xFF;
      }
    }
    in.readInt(); // templatePoolBytes
    layout.fsstDictId = Utils.getVarLong(in);
    layout.bodyTotalLen = in.readInt();
    final long bodyStart = in.position();
    layout.metaRawLen = in.readInt();
    layout.metaEncLen = in.readInt();
    layout.metaCodec = in.readByte() & 0xFF;
    in.readLong(); // META checksum
    layout.chunkTableOffset = in.position();
    final int chunkCount = in.readByte() & 0xFF;
    layout.chunkCount = chunkCount;
    layout.chunkFirstEntry = new int[chunkCount];
    layout.chunkEntryCount = new int[chunkCount];
    layout.chunkRawLen = new int[chunkCount];
    layout.chunkEncLen = new int[chunkCount];
    layout.chunkCodec = new int[chunkCount];
    layout.chunkRowOffset = new long[chunkCount];
    layout.chunkPayloadOffset = new long[chunkCount];
    for (int c = 0; c < chunkCount; c++) {
      layout.chunkRowOffset[c] = in.position();
      layout.chunkFirstEntry[c] = in.readShort() & 0xFFFF;
      layout.chunkEntryCount[c] = in.readShort() & 0xFFFF;
      layout.chunkRawLen[c] = in.readInt();
      layout.chunkEncLen[c] = in.readInt();
      layout.chunkCodec[c] = in.readByte() & 0xFF;
      in.readLong(); // chunk checksum
    }
    layout.metaPayloadOffset = in.position();
    long payload = layout.metaPayloadOffset + layout.metaEncLen;
    for (int c = 0; c < chunkCount; c++) {
      layout.chunkPayloadOffset[c] = payload;
      payload += layout.chunkEncLen[c];
    }
    layout.regionTableOffset = bodyStart + layout.bodyTotalLen;
    assertEquals(layout.regionTableOffset, payload, "the frames must fill exactly the declared body");
    return layout;
  }

  /** Offset of a chunk row's checksum field, for the sabotage suite. */
  static long chunkHashOffset(final ChunkedLayout layout, final int chunk) {
    return layout.chunkRowOffset[chunk] + 2 + 2 + 4 + 4 + 1;
  }
}
