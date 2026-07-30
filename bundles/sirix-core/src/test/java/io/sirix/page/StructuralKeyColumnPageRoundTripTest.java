/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.LE;
import io.sirix.node.Utils;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.LinkedHashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Round-trip tests for the structural-key columns in
 * {@link PageKind#KEYVALUELEAFPAGE}'s wire format.
 *
 * <p>When a column activates, the writer deletes that field's varint from every participating
 * record and the reader has to put back bytes that are not on disk anywhere. Getting the
 * participation predicate, the width, or the insertion offset even slightly out of step between
 * the two sides does not throw on the write — it produces a page that reads back wrong. These
 * tests therefore assert byte-identical records across a serialize/deserialize cycle, and read
 * the {@code structuralFlags} byte straight off the wire so what the page actually did is checked
 * rather than assumed.
 *
 * <p>They run against whichever way {@link PageKind#isRightSibKeyColumnEnabled()} is set — the
 * inline path by default, the column path under
 * {@code -Dsirix.rightSiblingKeyColumn.enable=true} — and both must be lossless.
 *
 * <p>Records are synthetic {@code ARRAY} (kindId 25) slots with genuine delta-varint structural
 * keys, shaped like the output of a DFS shred: siblings run consecutively and the last child of
 * each group has no right sibling. {@code ARRAY} is used because it carries every structural key
 * plus a hash without being one of the fused {@code OBJECT_NAMED_*} kinds, so the columns and
 * hash elision are exercised without dragging in the region-table second passes.
 */
@DisplayName("Structural-key column page round trip")
final class StructuralKeyColumnPageRoundTripTest {

  private static final long NULL = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** ARRAY, the kind these tests synthesise. */
  private static final int ARRAY_KIND_ID = 25;

  /** Offset of the {@code structuralFlags} byte's bit for the parentKey column. */
  private static final int FLAG_PARENT_KEY_COLUMN = 0x02;

  private static MemorySegmentAllocator allocator;

  @BeforeAll
  static void setUpClass() {
    allocator = Allocators.getInstance();
    allocator.init(8L * 1024 * 1024 * 1024); // 8 GiB
  }

  @Test
  @DisplayName("a DFS-shaped page columnarises its right siblings and round-trips byte-identical")
  void dfsShapedPageRoundTrips() {
    final int slots = 900;
    final long pageKey = 7L;
    final byte[][] expected = new byte[slots][];
    for (int slot = 0; slot < slots; slot++) {
      final long nodeKey = nodeKey(pageKey, slot);
      // Sibling groups of eight: the last of each group has no right sibling, the rest point
      // at the next node key. Every eleventh node opens a child group, so its right sibling is
      // a forward jump rather than its neighbour.
      final boolean lastOfGroup = (slot % 8) == 7;
      final boolean interior = (slot % 11) == 3;
      final long rightSib = lastOfGroup ? NULL : (interior ? nodeKey + 5 : nodeKey + 1);
      expected[slot] = arrayRecord(nodeKey,
          /* parentKey */ nodeKey(pageKey, (slot / 8) * 8),
          rightSib,
          /* leftSibKey */ (slot % 8) == 0 ? NULL : nodeKey - 1,
          /* firstChildKey */ interior ? nodeKey + 1 : NULL,
          /* lastChildKey */ interior ? nodeKey + 4 : NULL,
          /* pathNodeKey */ 3 + (slot % 4),
          /* hash */ 0L);
    }

    assertParentKeyColumnActivates(pageKey, expected, "a DFS-shaped page");
    assertRoundTrips(pageKey, expected);
  }

  /**
   * A page whose right siblings are all {@code NULL} — every node a lone child. The column
   * collapses to its all-null form, which means the writer strips a multi-byte varint from every
   * record and the reader rebuilds all of them out of a three-byte column. It is also the case a
   * value-based participation test would get wrong, since "no right sibling" and "kind has no
   * right-sibling field" are then indistinguishable by value.
   */
  @Test
  @DisplayName("an all-NULL right-sibling column still restores every record")
  void allNullRightSiblingsRoundTrip() {
    final int slots = 400;
    final long pageKey = 2L;
    final byte[][] expected = new byte[slots][];
    for (int slot = 0; slot < slots; slot++) {
      final long nodeKey = nodeKey(pageKey, slot);
      expected[slot] = arrayRecord(nodeKey, nodeKey(pageKey, 0), NULL, NULL, NULL, NULL,
          /* pathNodeKey */ 5, /* hash */ 0L);
    }

    assertParentKeyColumnActivates(pageKey, expected, "an all-NULL right-sibling column");
    assertRoundTrips(pageKey, expected);
  }

  /**
   * Node keys come from slot bits, not from the entry index, so a page with holes in its bitmap
   * feeds the column codec a non-consecutive predictor sequence. If either side derived node keys
   * by counting entries instead of walking the bitmap, this is where the two would diverge.
   */
  @Test
  @DisplayName("a sparsely populated page round-trips byte-identical")
  void sparselyPopulatedPageRoundTrips() {
    final long pageKey = 11L;
    final byte[][] expected = new byte[Constants.NDP_NODE_COUNT][];
    final Random rnd = new Random(0x5A5E);
    for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot += 1 + rnd.nextInt(4)) {
      final long nodeKey = nodeKey(pageKey, slot);
      expected[slot] = arrayRecord(nodeKey, nodeKey(pageKey, slot / 16),
          rnd.nextInt(4) == 0 ? NULL : nodeKey + 1 + rnd.nextInt(3),
          slot == 0 ? NULL : nodeKey - 1, NULL, NULL,
          /* pathNodeKey */ 1 + rnd.nextInt(6), /* hash */ 0L);
    }
    assertRoundTrips(pageKey, expected);
  }

  /**
   * Hashes that are not all-zero disable hash elision, which shifts every field after the hash
   * and leaves a different set of ranges for the strip and inject loops to reconcile. Node keys
   * far enough apart that their right-sibling deltas need several varint bytes also make the
   * column's payoff test the deciding factor rather than a formality.
   */
  @Test
  @DisplayName("non-elided hashes and wide deltas round-trip byte-identical")
  void nonZeroHashesAndWideDeltasRoundTrip() {
    final int slots = 500;
    final long pageKey = 4096L;
    final byte[][] expected = new byte[slots][];
    final Random rnd = new Random(0xDEAFBEE);
    for (int slot = 0; slot < slots; slot++) {
      final long nodeKey = nodeKey(pageKey, slot);
      expected[slot] = arrayRecord(nodeKey,
          /* parentKey */ nodeKey - 1_000_000L - rnd.nextInt(1000),
          /* rightSibKey */ (slot % 5) == 4 ? NULL : nodeKey + 1,
          /* leftSibKey */ (slot % 5) == 0 ? NULL : nodeKey - 1,
          NULL, NULL,
          /* pathNodeKey */ 2 + (slot % 3),
          /* hash */ rnd.nextLong() | 1L);
    }
    assertRoundTrips(pageKey, expected);
  }

  /**
   * A page of one record cannot amortise a column's four-byte length prefix, so the writer must
   * decline both columns and leave the record's varints inline. The reader has to agree, which it
   * only does if it reads the participation off the structural flags rather than assuming.
   */
  @Test
  @DisplayName("a page too small for a column to pay keeps its keys inline")
  void columnDeclinedOnTinyPage() {
    final long pageKey = 1L;
    final byte[][] expected = new byte[1][];
    expected[0] = arrayRecord(nodeKey(pageKey, 0), NULL, NULL, NULL, NULL, NULL, 1, 0L);

    final int flags = structuralFlagsOf(pageKey, expected);
    assertEquals(0, flags & FLAG_PARENT_KEY_COLUMN,
        "a single-record page cannot pay for a column; flags=0x" + Integer.toHexString(flags));
    assertRoundTrips(pageKey, expected);
  }

  // ==================== helpers ====================

  /**
   * Assert the parentKey column really fires on a page shaped for it.
   *
   * <p>Read off the wire rather than assumed, so a page that quietly stopped columnarising fails
   * instead of passing vacuously — which matters most for the participation predicate, whose
   * whole job is to keep writer and reader agreeing about which slots had bytes removed.
   */
  private static void assertParentKeyColumnActivates(final long pageKey, final byte[][] records,
      final String what) {
    final int flags = structuralFlagsOf(pageKey, records);
    assertTrue((flags & FLAG_PARENT_KEY_COLUMN) != 0,
        "parentKey column did not activate on " + what + "; flags=0x"
            + Integer.toHexString(flags));
  }

  /** Serialize a page built from {@code records}, deserialize it, and compare every slot. */
  private static void assertRoundTrips(final long pageKey, final byte[][] records) {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage orig = newPage(pageKey, config, records);
    KeyValueLeafPage deserialized = null;
    try {
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, orig, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte(); // skip pageKind id
      deserialized = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE
          .deserializePage(config, source, SerializationType.DATA);

      for (int slot = 0; slot < records.length; slot++) {
        assertArrayEquals(records[slot], deserialized.getSlotAsByteArray(slot),
            "slot " + slot + " mismatch");
      }
    } finally {
      orig.close();
      if (deserialized != null) {
        deserialized.close();
      }
    }
  }

  /**
   * Serialize a page and return its {@code structuralFlags} byte, parsed straight off the wire.
   * Reading the flags rather than inferring them is what makes an activation assertion mean
   * something: the page envelope is {@code version, flags, varLong pageKey, int revision, byte
   * indexType, 160 bytes header+bitmap, int populatedCount, int onDiskHeapSize, byte
   * templateCount, byte structuralFlags}.
   */
  private static int structuralFlagsOf(final long pageKey, final byte[][] records) {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(pageKey, config, records);
    try {
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte(); // pageKind id
      source.readByte(); // binary encoding version
      source.readByte(); // envelope flags
      assertEquals(pageKey, Utils.getVarLong(source));
      source.readInt();  // revision
      assertEquals(IndexType.DOCUMENT, IndexType.getType(source.readByte()));
      source.read(new byte[PageLayout.DISK_HEADER_BITMAP_SIZE]);
      source.readInt();  // populatedCount
      source.readInt();  // onDiskHeapSize
      final int templateCount = source.readByte() & 0xFF;
      assertNotEquals(0, templateCount,
          "offset-table dedup fell back to the inline path, so no column can be present");
      return source.readByte() & 0xFF;
    } finally {
      page.close();
    }
  }

  private static KeyValueLeafPage newPage(final long pageKey, final ResourceConfiguration config,
      final byte[][] records) {
    int total = 0;
    for (final byte[] record : records) {
      if (record != null) {
        total += record.length;
      }
    }
    final KeyValueLeafPage page = new KeyValueLeafPage(pageKey, 0, IndexType.DOCUMENT, config,
        false, null, new LinkedHashMap<>(), allocator.allocate(PageLayout.HEAP_START + total + 64),
        null, -1);
    for (int slot = 0; slot < records.length; slot++) {
      if (records[slot] != null) {
        page.setSlotWithNodeKind(MemorySegment.ofArray(records[slot]), slot, ARRAY_KIND_ID);
      }
    }
    return page;
  }

  private static long nodeKey(final long pageKey, final int slot) {
    return (pageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
  }

  /**
   * Build one {@code ARRAY} record: {@code [kindId][offsetTable: 11 bytes][data]}, with the
   * structural keys written as real delta-varints against {@code nodeKey} so decoding and
   * re-encoding them reproduces the original bytes exactly. The three fields the columns never
   * touch (both revisions and both counts) are single zero bytes — the strip and inject loops
   * copy them opaquely, so only their widths matter.
   */
  private static byte[] arrayRecord(final long nodeKey, final long parentKey, final long rightSibKey,
      final long leftSibKey, final long firstChildKey, final long lastChildKey,
      final long pathNodeKey, final long hash) {
    final byte[] data = new byte[128];
    final MemorySegment dataSeg = MemorySegment.ofArray(data);
    final int[] offsets = new int[NodeFieldLayout.ARRAY_FIELD_COUNT];
    int cursor = 0;

    offsets[NodeFieldLayout.ARRAY_PARENT_KEY] = cursor;
    cursor += DeltaVarIntCodec.writeDeltaToSegment(dataSeg, cursor, parentKey, nodeKey);
    offsets[NodeFieldLayout.ARRAY_RIGHT_SIB_KEY] = cursor;
    cursor += DeltaVarIntCodec.writeDeltaToSegment(dataSeg, cursor, rightSibKey, nodeKey);
    offsets[NodeFieldLayout.ARRAY_LEFT_SIB_KEY] = cursor;
    cursor += DeltaVarIntCodec.writeDeltaToSegment(dataSeg, cursor, leftSibKey, nodeKey);
    offsets[NodeFieldLayout.ARRAY_FIRST_CHILD_KEY] = cursor;
    cursor += DeltaVarIntCodec.writeDeltaToSegment(dataSeg, cursor, firstChildKey, nodeKey);
    offsets[NodeFieldLayout.ARRAY_LAST_CHILD_KEY] = cursor;
    cursor += DeltaVarIntCodec.writeDeltaToSegment(dataSeg, cursor, lastChildKey, nodeKey);
    offsets[NodeFieldLayout.ARRAY_PATH_NODE_KEY] = cursor;
    cursor += DeltaVarIntCodec.writeDeltaToSegment(dataSeg, cursor, pathNodeKey, nodeKey);
    offsets[NodeFieldLayout.ARRAY_PREV_REVISION] = cursor;
    cursor += 1;
    offsets[NodeFieldLayout.ARRAY_LAST_MOD_REVISION] = cursor;
    cursor += 1;
    offsets[NodeFieldLayout.ARRAY_HASH] = cursor;
    dataSeg.set(LE.LONG, cursor, hash);
    cursor += NodeFieldLayout.HASH_WIDTH;
    offsets[NodeFieldLayout.ARRAY_CHILD_COUNT] = cursor;
    cursor += 1;
    offsets[NodeFieldLayout.ARRAY_DESCENDANT_COUNT] = cursor;
    cursor += 1;

    final byte[] record = new byte[1 + NodeFieldLayout.ARRAY_FIELD_COUNT + cursor];
    record[0] = (byte) ARRAY_KIND_ID;
    for (int f = 0; f < NodeFieldLayout.ARRAY_FIELD_COUNT; f++) {
      record[1 + f] = (byte) offsets[f];
    }
    System.arraycopy(data, 0, record, 1 + NodeFieldLayout.ARRAY_FIELD_COUNT, cursor);
    return record;
  }

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
  }
}
