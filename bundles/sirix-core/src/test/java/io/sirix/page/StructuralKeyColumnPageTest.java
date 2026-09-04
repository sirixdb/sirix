/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.Utils;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witness for the right- and left-sibling structural columns — and for the measurement that keeps
 * them off by default.
 *
 * <p>
 * The columns work: they strip both sibling varints out of every record and put them back byte for
 * byte, under the same offset-table predicate the parentKey column uses. What they do not do is
 * make the page smaller. In document order both keys are a constant delta from the node key, so the
 * varints are the same two bytes in every record and the body codec was already removing them for
 * nothing. Measured on a 92-page JSON record load the columns take 930 B per page out of the raw
 * heap and put 14 B per page back on the wire; on the synthetic page below, whose sibling keys are
 * exactly ±1, they come out about 21 bytes ahead. Either way what reaches disk barely moves. This
 * suite pins both halves: correctness, so the mechanism can be turned on the day the arithmetic
 * changes, and the magnitude, so nobody turns it on believing it shrinks anything.
 */
@DisplayName("Structural sibling-key columns")
final class StructuralKeyColumnPageTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  /** Slots the record-shaped fixture populates. */
  private static final int SLOTS = 500;

  /** Fields per record, repeated across the page the way a record load lays them out. */
  private static final int FIELDS = 25;

  private Arena arena;
  private boolean siblingColumnsBefore;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    siblingColumnsBefore = PageKind.SIBLING_KEY_COLUMNS_ENABLED;
  }

  @AfterEach
  void tearDown() {
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = siblingColumnsBefore;
    if (arena != null) {
      arena.close();
    }
  }

  @Test
  @DisplayName("the lever is off by default, because it does not shrink the page")
  void theLeverIsOffByDefault() {
    assertEquals("false", System.getProperty("sirix.page.body.structuralColumns", "false"),
        "this suite reasons about the default; run it without the property set");
    assertEquals(false, siblingColumnsBefore,
        "the sibling columns default off — see PageKind.SIBLING_KEY_COLUMNS_ENABLED for the numbers");
  }

  @Test
  @DisplayName("every record round-trips with the columns on, byte for byte")
  void recordsRoundTripWithTheColumnsOn() {
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = true;
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    try {
      fillRecordShaped(page);
      back = roundTrip(config, page);
      for (int slot = 0; slot < SLOTS; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), back.getSlotAsByteArray(slot), "slot " + slot);
      }
    } finally {
      if (back != null) {
        back.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("a page written with the columns reads back with the switch off, and the other way round")
  void bothFormsReadUnderEitherSetting() {
    final ResourceConfiguration config = newConfig();

    PageKind.SIBLING_KEY_COLUMNS_ENABLED = true;
    final byte[] withColumns = serializeRecordShapedPage(config);
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = false;
    final byte[] inline = serializeRecordShapedPage(config);

    assertNotEquals(hex(inline), hex(withColumns), "the switch must actually change the bytes");

    // A page states in its own structural flags which form it took, so a reader configured either way
    // has to read both. That is what lets one resource hold pages of each.
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = false;
    final byte[][] columnsReadWithSwitchOff = slotsOf(config, withColumns);
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = true;
    final byte[][] inlineReadWithSwitchOn = slotsOf(config, inline);
    for (int slot = 0; slot < SLOTS; slot++) {
      assertArrayEquals(inlineReadWithSwitchOn[slot], columnsReadWithSwitchOff[slot], "slot " + slot);
    }
  }

  @Test
  @DisplayName("the columns strip the heap and barely move the wire — the reason the default is off")
  void theColumnsStripTheHeapAndBarelyMoveTheWire() {
    final ResourceConfiguration config = newConfig();

    PageKind.SIBLING_KEY_COLUMNS_ENABLED = false;
    final byte[] inline = serializeRecordShapedPage(config);
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = true;
    final byte[] withColumns = serializeRecordShapedPage(config);

    // Both halves of the finding, so a change that flips either one is caught. The heap the records
    // take shrinks by both sibling varints per slot — a large, unambiguous win in raw bytes.
    final int inlineHeap = onDiskHeapSize(inline);
    final int columnHeap = onDiskHeapSize(withColumns);
    assertTrue(columnHeap < inlineHeap - SLOTS,
        "the columns must strip at least one byte of heap per slot — " + columnHeap + " vs " + inlineHeap);
    // And almost none of it reaches the wire, because the varints removed were the same two bytes in
    // every record and the codec was already collapsing them. Which way the remainder tips is
    // data-dependent: this synthetic page, whose sibling keys are exactly ±1, comes out ~21 bytes
    // (2.5 %) SMALLER once the column codec's run-length lane is in play; a 92-page JSON record load,
    // whose keys are not that regular, still comes out ~14 bytes per page LARGER. So the assertion is
    // on the magnitude, which is the finding that keeps the default off: whatever the raw heap says,
    // this lever does not move the page by anything worth the format surface.
    final int wireDelta = Math.abs(withColumns.length - inline.length);
    assertTrue(wireDelta * 20 < inline.length, "the wire must barely move, or the premise behind the default is stale: "
        + withColumns.length + " vs " + inline.length);
    assertTrue(inlineHeap - columnHeap > wireDelta * 20, "the raw heap saving must dwarf what reaches the wire — heap "
        + (inlineHeap - columnHeap) + " B, wire " + wireDelta + " B");
  }

  // ──────────────────────────────────────────────────────────────── helpers

  /** The {@code onDiskHeapSize} field of a record page's body prefix. */
  private static int onDiskHeapSize(final byte[] wire) {
    // Envelope: page-kind id, binary version, flags, varint page key, revision int, index-type byte,
    // then the 160-byte header and bitmap, then populatedCount and onDiskHeapSize.
    final BytesIn<?> in = Bytes.elasticOffHeapByteBuffer().write(wire).bytesForRead();
    in.readByte();
    in.readByte();
    in.readByte();
    Utils.getVarLong(in);
    in.readInt();
    in.readByte();
    in.skip(PageLayout.DISK_HEADER_BITMAP_SIZE);
    in.readInt();
    return in.readInt();
  }

  private byte[] serializeRecordShapedPage(final ResourceConfiguration config) {
    final KeyValueLeafPage page = newPage(config);
    try {
      fillRecordShaped(page);
      PageKind.resetStickyCodecElectionForCurrentThread();
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      return sink.toByteArray();
    } finally {
      page.close();
    }
  }

  private byte[][] slotsOf(final ResourceConfiguration config, final byte[] wire) {
    final BytesIn<?> source = Bytes.elasticOffHeapByteBuffer().write(wire).bytesForRead();
    source.readByte();
    final KeyValueLeafPage back =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
    try {
      final byte[][] slots = new byte[SLOTS][];
      for (int slot = 0; slot < SLOTS; slot++) {
        slots[slot] = back.getSlotAsByteArray(slot);
      }
      return slots;
    } finally {
      back.close();
    }
  }

  /**
   * Records in document order: each slot's right sibling is the next node key and its left sibling
   * the previous one, except at the record boundaries, which is the shape the column codec's node-key
   * predictors are built for.
   */
  private void fillRecordShaped(final KeyValueLeafPage page) {
    for (int i = 0; i < SLOTS; i++) {
      final boolean lastOfRecord = (i % FIELDS) == FIELDS - 1;
      final boolean firstOfRecord = (i % FIELDS) == 0;
      final long rightSibling = lastOfRecord
          ? Fixed.NULL_NODE_KEY.getStandardProperty()
          : i + 1;
      final long leftSibling = firstOfRecord
          ? Fixed.NULL_NODE_KEY.getStandardProperty()
          : i - 1;
      writeNumber(page, i, 200 + (i % FIELDS), 900 + (i % FIELDS), rightSibling, leftSibling, i * 3);
    }
  }

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder("structuralKeyColumns").buildPathSummary(true).build();
  }

  private KeyValueLeafPage newPage(final ResourceConfiguration config) {
    return new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, arena.allocate(MemorySegmentAllocator.SIXTYFOUR_KB),
        null);
  }

  private static KeyValueLeafPage roundTrip(final ResourceConfiguration config, final KeyValueLeafPage page) {
    PageKind.resetStickyCodecElectionForCurrentThread();
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    final BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
  }

  private void writeNumber(final KeyValueLeafPage page, final long nodeKey, final int nameKey, final long pathNodeKey,
      final long rightSibling, final long leftSibling, final int value) {
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey, 0L, rightSibling, leftSibling, nameKey,
        pathNodeKey, 0, 0, 0L, Integer.valueOf(value), HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  private static String hex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      sb.append(Character.forDigit((b >>> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
