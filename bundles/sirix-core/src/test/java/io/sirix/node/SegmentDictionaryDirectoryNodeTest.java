/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.SegmentDictionaryDirectoryNode.SlotTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The segment dictionary directory at key 1 of the projection value dictionary sub-trie: page key
 * -> segment, (segment, slot) -> sealed generation, tag -> slot, its wire round trip, and the
 * refusals that keep one value from ever getting two ids in one segment.
 */
final class SegmentDictionaryDirectoryNodeTest {

  private static final long KEY = SegmentDictionaryDirectoryNode.DIRECTORY_KEY;

  private static SegmentDictionaryDirectoryNode roundTrip(final SegmentDictionaryDirectoryNode node) {
    try (final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.SEGMENT_DICTIONARY_DIRECTORY.serialize(sink, node, null);
      return (SegmentDictionaryDirectoryNode) NodeKind.SEGMENT_DICTIONARY_DIRECTORY.deserialize(
          Bytes.wrapForRead(sink.toByteArray()), node.getNodeKey(), null, null);
    }
  }

  private static byte[] serialize(final SegmentDictionaryDirectoryNode node) {
    try (final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.SEGMENT_DICTIONARY_DIRECTORY.serialize(sink, node, null);
      return sink.toByteArray();
    }
  }

  private static SegmentDictionaryDirectoryNode deserialize(final byte[] bytes) {
    return (SegmentDictionaryDirectoryNode) NodeKind.SEGMENT_DICTIONARY_DIRECTORY.deserialize(
        Bytes.wrapForRead(bytes), KEY, null, null);
  }

  /** Three segments; the middle one sealed on two slots with a placeholder between, the last unsealed. */
  private static SegmentDictionaryDirectoryNode sample() {
    final SlotTable first = SlotTable.takeOwnership(new int[][] {{7}}, new long[] {1024L}, new int[] {12});
    final SlotTable second = SlotTable.takeOwnership(
        new int[][] {{3, 9}, {}, {12}},
        new long[] {2048L, 0L, 4096L},
        new int[] {275_494, 0, 1});
    return SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[] {0L, 1000L, 50_000L},
        new SlotTable[] {first, second, SlotTable.EMPTY});
  }

  @Test
  @DisplayName("segmentOf answers the last segment whose start does not exceed the page key")
  void segmentOfIsTheCoveringSegment() {
    final SegmentDictionaryDirectoryNode node = sample();
    assertEquals(3, node.segmentCount());
    assertEquals(0, node.segmentOf(0L));
    assertEquals(0, node.segmentOf(999L));
    assertEquals(1, node.segmentOf(1000L));
    assertEquals(1, node.segmentOf(49_999L));
    assertEquals(2, node.segmentOf(50_000L));
    assertEquals(2, node.segmentOf(Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> node.segmentOf(-1L));
    assertEquals(1000L, node.segmentStart(1));
    assertArrayEquals(new long[] {0L, 1000L, 50_000L}, node.segmentStarts());
  }

  @Test
  @DisplayName("a single-segment directory maps every page key to segment 0")
  void singleSegment() {
    final SegmentDictionaryDirectoryNode node =
        SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[] {0L}, new SlotTable[] {SlotTable.EMPTY});
    assertEquals(0, node.segmentOf(0L));
    assertEquals(0, node.segmentOf(Long.MAX_VALUE));
    assertEquals(0L, node.headerKey(0, 0));
    assertEquals(0, node.entryCount(0, 5));
    assertEquals(-1, node.slots(0).slotOfTag(0));
  }

  @Test
  @DisplayName("slots answer their sealed generation, placeholders and unknown slots answer zero")
  void slotLookups() {
    final SegmentDictionaryDirectoryNode node = sample();
    assertEquals(1024L, node.headerKey(0, 0));
    assertEquals(12, node.entryCount(0, 0));
    assertEquals(0L, node.headerKey(0, 1), "a slot beyond the table is not an error, it is absent");
    assertEquals(0, node.entryCount(0, -1));

    final SlotTable second = node.slots(1);
    assertEquals(3, second.slotCount());
    assertEquals(2048L, second.headerKey(0));
    assertEquals(275_494, second.entryCount(0));
    assertEquals(0L, second.headerKey(1), "the placeholder slot");
    assertEquals(0, second.entryCount(1));
    assertEquals(0, second.tags(1).length);
    assertEquals(4096L, second.headerKey(2));
    assertArrayEquals(new int[] {3, 9}, second.tags(0));
    assertEquals(0, second.tags(7).length, "tags of an unknown slot are empty, never null");

    assertEquals(0, second.slotOfTag(3));
    assertEquals(0, second.slotOfTag(9));
    assertEquals(2, second.slotOfTag(12));
    assertEquals(-1, second.slotOfTag(7), "tag 7 is slot 0 of segment 0, not of segment 1");
    assertEquals(-1, second.slotOfTag(4));

    assertSame(SlotTable.EMPTY, node.slots(2));
    assertEquals(0, SlotTable.EMPTY.slotCount());
  }

  /**
   * A SLOT beyond a table is absent (a segment may legitimately have fewer slots than a tag asks
   * for), but a SEGMENT beyond the directory is a caller error: the directory knows every segment
   * that exists, so asking for one it does not hold means the caller's segment arithmetic is wrong,
   * and answering "absent" would let a lane read bytes where a dictionary was expected.
   */
  @Test
  @DisplayName("a segment index outside the directory is refused on every accessor, a slot is not")
  void segmentIndexIsChecked() {
    final SegmentDictionaryDirectoryNode node = sample();
    assertEquals(3, node.segmentCount());
    for (final int outside : new int[] {-1, 3, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
      assertThrows(IndexOutOfBoundsException.class, () -> node.segmentStart(outside), "segmentStart(" + outside + ")");
      assertThrows(IndexOutOfBoundsException.class, () -> node.slots(outside), "slots(" + outside + ")");
      assertThrows(IndexOutOfBoundsException.class, () -> node.headerKey(outside, 0), "headerKey(" + outside + ")");
      assertThrows(IndexOutOfBoundsException.class, () -> node.entryCount(outside, 0), "entryCount(" + outside + ")");
    }
    // The boundary segment is in: the last one answers, only the one past it refuses.
    assertEquals(50_000L, node.segmentStart(2));
    assertEquals(0L, node.headerKey(2, 0));
    assertEquals(0, node.entryCount(2, Integer.MAX_VALUE), "a slot index is lenient whatever its size");
  }

  @Test
  @DisplayName("the directory round-trips through the record codec, placeholders and EMPTY included")
  void roundTrips() {
    final SegmentDictionaryDirectoryNode read = roundTrip(sample());
    assertEquals(KEY, read.getNodeKey());
    assertEquals(NodeKind.SEGMENT_DICTIONARY_DIRECTORY, read.getKind());
    assertArrayEquals(new long[] {0L, 1000L, 50_000L}, read.segmentStarts());
    assertEquals(1, read.slots(0).slotCount());
    assertEquals(1024L, read.headerKey(0, 0));
    assertEquals(12, read.entryCount(0, 0));
    assertArrayEquals(new int[] {7}, read.slots(0).tags(0));
    final SlotTable second = read.slots(1);
    assertEquals(3, second.slotCount());
    assertArrayEquals(new int[] {3, 9}, second.tags(0));
    assertEquals(0, second.tags(1).length);
    assertArrayEquals(new int[] {12}, second.tags(2));
    assertEquals(2048L, second.headerKey(0));
    assertEquals(0L, second.headerKey(1));
    assertEquals(4096L, second.headerKey(2));
    assertEquals(275_494, second.entryCount(0));
    assertEquals(0, second.entryCount(1));
    assertEquals(1, second.entryCount(2));
    assertSame(SlotTable.EMPTY, read.slots(2), "an unsealed segment reads back as THE empty table");
    // Read once more: serializing the read copy must give identical bytes.
    assertArrayEquals(serialize(sample()), serialize(read));
  }

  @Test
  @DisplayName("the directory refuses a wrong key, a wrong first start, or non-ascending starts")
  void directoryShapeIsChecked() {
    final SlotTable[] one = {SlotTable.EMPTY};
    final SlotTable[] two = {SlotTable.EMPTY, SlotTable.EMPTY};
    assertThrows(IllegalArgumentException.class,
        () -> SegmentDictionaryDirectoryNode.takeOwnership(2L, new long[] {0L}, one), "the directory lives at key 1");
    assertThrows(IllegalArgumentException.class,
        () -> SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[] {1L}, one), "segment 0 starts at page 0");
    assertThrows(IllegalArgumentException.class,
        () -> SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[] {0L, 5L, 5L},
            new SlotTable[] {SlotTable.EMPTY, SlotTable.EMPTY, SlotTable.EMPTY}), "equal starts");
    assertThrows(IllegalArgumentException.class,
        () -> SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[] {0L, 9L, 5L},
            new SlotTable[] {SlotTable.EMPTY, SlotTable.EMPTY, SlotTable.EMPTY}), "descending starts");
    assertThrows(IllegalArgumentException.class,
        () -> SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[0], new SlotTable[0]), "no segments");
    assertThrows(IllegalArgumentException.class,
        () -> SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[] {0L, 5L}, one), "length mismatch");
    assertThrows(NullPointerException.class,
        () -> SegmentDictionaryDirectoryNode.takeOwnership(KEY, new long[] {0L, 5L}, new SlotTable[] {null, null}));
    assertThrows(NullPointerException.class, () -> SegmentDictionaryDirectoryNode.takeOwnership(KEY, null, two));
  }

  @Test
  @DisplayName("a slot table refuses every shape that would give a value two ids or a phantom dictionary")
  void slotTableShapeIsChecked() {
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{1}, {1}}, new long[] {10L, 20L}, new int[] {1, 1}),
        "one tag in two slots");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{1, 5}, {3, 5}}, new long[] {10L, 20L}, new int[] {1, 1}),
        "a duplicate anywhere in the merged tag set");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{5, 3}}, new long[] {10L}, new int[] {1}), "tags must ascend");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{3, 3}}, new long[] {10L}, new int[] {1}), "strictly");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{-1}}, new long[] {10L}, new int[] {1}), "negative tag");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{1}}, new long[] {10L}, new int[] {0}),
        "a header without entries");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{1}}, new long[] {0L}, new int[] {3}),
        "entries without a header");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{}}, new long[] {10L}, new int[] {3}),
        "a dictionary covering no tag");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{1}}, new long[] {-10L}, new int[] {3}), "negative header key");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{1}}, new long[] {10L}, new int[] {-3}), "negative count");
    assertThrows(IllegalArgumentException.class,
        () -> SlotTable.takeOwnership(new int[][] {{1}, {2}}, new long[] {10L}, new int[] {1, 1}),
        "arrays of different lengths");
    assertThrows(NullPointerException.class,
        () -> SlotTable.takeOwnership(new int[][] {null}, new long[] {0L}, new int[] {0}), "null tags");
    assertThrows(NullPointerException.class, () -> SlotTable.takeOwnership(null, new long[0], new int[0]));
    // Legal: a tag-less, dictionary-less placeholder; and a tagged slot not (yet) sealed.
    final SlotTable legal = SlotTable.takeOwnership(new int[][] {{}, {4}}, new long[] {0L, 0L}, new int[] {0, 0});
    assertEquals(2, legal.slotCount());
    assertEquals(1, legal.slotOfTag(4));
  }

  @Test
  @DisplayName("the deserializer refuses claimed counts that cannot fit the record")
  void deserializerRefusesOverruns() {
    final byte[] good = serialize(sample());
    assertEquals(3, deserialize(good).segmentCount());
    // Layout: segments:int(0..3), starts:long x3 (4..27), then per segment slots:int ...
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 0, 0)), "0 segments");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 0, -1)), "negative segments");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 0, 1 << 30)),
        "a segment count the record cannot hold is refused before anything is allocated");
    assertThrows(IllegalStateException.class,
        () -> deserialize(withInt(good, 0, SegmentDictionaryDirectoryNode.MAX_SEGMENTS + 1)), "above the cap");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 0, 1000)),
        "within the cap but 1000 starts do not fit this record");
    // Segment 0's slot count sits right after the three starts.
    final int slotCountAt = 4 + 3 * Long.BYTES;
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, slotCountAt, -1)), "negative slots");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, slotCountAt, 1 << 20)),
        "above the slot cap");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, slotCountAt, 60_000)),
        "within the cap but 60000 slots do not fit this record");
    // Segment 0, slot 0's tag count follows the slot count.
    final int tagCountAt = slotCountAt + 4;
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, tagCountAt, -1)), "negative tags");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, tagCountAt, 1 << 28)),
        "above the tag cap");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, tagCountAt, 1000)),
        "within the cap but 1000 tags do not fit this record");
    // Fewer segments than the starts claim mis-frames everything after: the bytes of start 2 are read
    // as a slot count (the low int of 50000L = 50000 slots), which cannot fit -- refused, not misread.
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 0, 2)));
    final byte[] truncated = new byte[good.length - 1];
    System.arraycopy(good, 0, truncated, 0, truncated.length);
    assertThrows(RuntimeException.class, () -> deserialize(truncated), "a truncated record never reads clean");
  }

  private static byte[] withInt(final byte[] bytes, final int at, final int value) {
    final byte[] copy = bytes.clone();
    // The record codecs are little-endian (LE.INT).
    copy[at] = (byte) value;
    copy[at + 1] = (byte) (value >>> 8);
    copy[at + 2] = (byte) (value >>> 16);
    copy[at + 3] = (byte) (value >>> 24);
    return copy;
  }
}
