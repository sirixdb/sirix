/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The inline record cap, raised 512 -> 1,023, and the one number it is.
 *
 * <p>
 * A record over the cap leaves the page for an {@link OverflowPage} and leaves an inline descriptor
 * behind, which costs a heap record plus a page reference and takes the value out of every column.
 * The old cap was a policy constant maintained apart from the wire's reach: the persisted compact
 * directory entry is two bytes with a 10-bit length, so 1,023 was always expressible and 512 threw
 * half of it away. The cap is now that ceiling, and the three places that name it are one
 * declaration and two aliases.
 *
 * <p>
 * Mutation: put either constant back to 512 and the boundary tests below fail on the record that
 * stops fitting; make them disagree and {@link PageLayout}'s initializer refuses to load.
 */
@DisplayName("Fused record size cap")
final class FusedRecordSizeCapTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  /** Bytes a fused OBJECT_NAMED_STRING record costs on top of its value on this fixture. */
  private static final int FUSED_STRING_RECORD_OVERHEAD = 29;

  private static MemorySegmentAllocator allocator;

  private Arena arena;

  @BeforeAll
  static void setUpClass() {
    allocator = Allocators.getInstance();
    allocator.init(8L * 1024 * 1024 * 1024); // 8 GiB
  }

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
  }

  @AfterEach
  void tearDown() {
    if (arena != null) {
      arena.close();
    }
  }

  @Test
  @DisplayName("the cap has one source of truth and two aliases")
  void theCapIsOneNumber() {
    assertEquals(1_023, Constants.MAX_RECORD_SIZE, "the cap is the compact directory's 10-bit length ceiling");
    assertEquals(Constants.MAX_RECORD_SIZE, PageConstants.MAX_RECORD_SIZE,
        "PageConstants must alias the settings constant, not restate it");
    assertEquals(Constants.MAX_RECORD_SIZE, PageLayout.MAX_COMPACT_DIR_DATA_LENGTH,
        "the cap and the wire directory's reach must be the same number");
    assertEquals(Constants.MAX_RECORD_SIZE, OverflowSlotSidecar.MAX_IMAGE_BYTES,
        "a side slot holds the record shape the heap would have held, so its ceiling is the same");
  }

  @Test
  @DisplayName("a 1,000-byte record stays inline; a 1,100-byte one still overflows")
  void theCapDecidesInlineVersusOverflow() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      writeString(page, 0, 100, "a".repeat(1_000 - FUSED_STRING_RECORD_OVERHEAD));
      writeString(page, 1, 101, "b".repeat(1_100 - FUSED_STRING_RECORD_OVERHEAD));
      assertEquals(1_000, page.getSlotAsByteArray(0).length,
          "the fixture must sit exactly on the intended side of the cap, not near it");

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);

      assertFalse(page.isFusedObjectNamedStringOverflowDescriptor(0),
          "a 1,000-byte record fits the raised cap; at 512 it was an overflow descriptor");
      assertTrue(page.isFusedObjectNamedStringOverflowDescriptor(1),
          "a 1,100-byte record is past the wire's reach and must still overflow");

      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      deserialized =
          (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
      assertArrayEquals(page.getSlotAsByteArray(0), deserialized.getSlotAsByteArray(0),
          "the newly-inline record has to survive the round trip, elision and all");
      assertArrayEquals(page.getSlotAsByteArray(1), deserialized.getSlotAsByteArray(1),
          "and so does the descriptor the oversized record left behind");
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("a raw slot at the cap round-trips and one byte past it is refused")
  void rawSlotsStopAtTheCap() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      final byte[] atTheCap = new byte[PageConstants.MAX_RECORD_SIZE];
      for (int i = 0; i < atTheCap.length; i++) {
        atTheCap[i] = (byte) i;
      }
      page.setSlot(atTheCap, 7);
      assertThrows(IllegalArgumentException.class, () -> page.setSlot(new byte[PageConstants.MAX_RECORD_SIZE + 1], 8),
          "raw slotted-page bytes above the cap cannot be expressed by the wire directory");

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      deserialized =
          (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
      assertArrayEquals(atTheCap, deserialized.getSlotAsByteArray(7),
          "the largest representable record must come back byte for byte");
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  /**
   * The cap decides a RECORD's fate, not the page's budget: the slotted page's backing memory still
   * stops at {@link KeyValueLeafPage#MAX_SLOTTED_PAGE_CAPACITY} (the allocator's last size class, 256
   * KiB), so 1,024 slots of cap-sized records cannot all be inline whatever the cap is. What doubling
   * the cap changes is how soon that ceiling arrives — roughly halving the records a full page holds
   * inline — so this packs a page with cap-sized records well past the ceiling and requires that the
   * diversion be graceful and lossless. {@code ensureInlineAppendCapacity} returns false rather than
   * growing past the last size class, and the record goes to the page's {@code OverflowSlotSidecar}
   * instead. Measured here: 253 of 1,024 cap-sized records stay in the heap (253,000 B) and 771
   * become side slots; at the old cap the same page held 495 inline. The sidecar needed no change —
   * its {@code MAX_IMAGE_BYTES} aliases the cap, its per-slot length is a {@code short} (1,023 is far
   * inside it), its 18-bit in-chunk offset is unaffected because one image never exceeds the cap, and
   * {@code MAX_LIVE_BYTES} (1,024 x cap = 1,047,552) still fits an {@code int} and only bounds a
   * growth heuristic.
   */
  @Test
  @DisplayName("a page packed with 1,000-byte records fills, diverts to the sidecar and round-trips")
  void aPagePackedWithCapSizedRecordsRoundTrips() {
    final ResourceConfiguration config = newConfig();
    // Start at the smallest size class so the page has to grow its way to the ceiling under load,
    // and let the page own the frame: growth releases the previous one back to this allocator.
    final KeyValueLeafPage page =
        new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, allocator.allocate(1), null, false);
    KeyValueLeafPage deserialized = null;
    try {
      final String value = "z".repeat(1_000 - FUSED_STRING_RECORD_OVERHEAD);
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        writeString(page, slot, 100, value);
      }

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);

      // The ceiling really was crossed, otherwise this witnesses nothing about the diversion.
      final int sideSlots = page.getSideSlotCount();
      assertTrue(sideSlots > 0,
          "1,024 cap-sized records cannot fit a 256 KiB page; some must have gone to the sidecar");
      assertTrue(sideSlots < Constants.NDP_NODE_COUNT,
          "and the ones written before the ceiling must still be in the heap");
      assertEquals(262_144, KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY,
          "the page's own ceiling is the allocator's last size class and the cap raise must not move it");

      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      deserialized =
          (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), deserialized.getSlotAsByteArray(slot),
            "slot " + slot + " did not survive the round trip");
      }
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder("fusedRecordSizeCap").build();
  }

  private KeyValueLeafPage newPage(final ResourceConfiguration config) {
    return new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, arena.allocate(MemorySegmentAllocator.SIXTYFOUR_KB),
        null);
  }

  private static void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final String value) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, false, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }
}
