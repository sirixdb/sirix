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
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.page.pax.PathNodeKeyRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witness for the compact pathNodeKey column.
 *
 * <p>
 * The column's random-access layout spends four bytes on every dictionary key — whose spread across
 * a page is usually under a hundred — and one byte on every slot's dictionary id, on a sequence
 * that in record-shaped data walks the same field order for every record and is therefore a handful
 * of constant-stride runs. Both are re-derivable, and both are measured rather than assumed: the
 * encoder keeps the compact form only where it comes out smaller, and a reader expands it back to
 * the random-access layout once per page so every per-slot lookup stays the single popcount it was.
 *
 * <p>
 * The compaction also made the column pay for itself on pages where it previously did not, which is
 * why the participation rule is witnessed here too: a slot whose pathNodeKey is the non-positive
 * "no path summary" sentinel must stay inline, because the reader's only way back reports -1 for
 * both "absent" and "stored -1".
 */
@DisplayName("Compact pathNodeKey column")
final class PathNodeKeyColumnCompactionTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  private Arena arena;
  private boolean compactBefore;
  private boolean derivedElisionBefore;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    compactBefore = PageKind.PATH_NODE_KEY_COLUMN_COMPACT;
    derivedElisionBefore = PageKind.DERIVED_ELISION_SECTIONS;
  }

  @AfterEach
  void tearDown() {
    PageKind.PATH_NODE_KEY_COLUMN_COMPACT = compactBefore;
    PageKind.DERIVED_ELISION_SECTIONS = derivedElisionBefore;
    if (arena != null) {
      arena.close();
    }
  }

  @Test
  @DisplayName("compaction round-trips every shape it accepts, byte for byte and lookup for lookup")
  void compactionRoundTripsEveryShapeItAccepts() {
    final Random random = new Random(20260830L);
    int accepted = 0;
    int cyclicAccepted = 0;
    for (int trial = 0; trial < 600; trial++) {
      final int count = 1 + random.nextInt(400);
      final int unique = 1 + random.nextInt(Math.min(count, 40));
      final int[] dictionary = new int[unique];
      // A base that is sometimes negative and keys that are sometimes far apart, so the frame of
      // reference has to cope with every dictionary width it can choose.
      final int base = random.nextInt(5) == 0
          ? -random.nextInt(1_000)
          : random.nextInt(100_000);
      for (int i = 0; i < unique; i++) {
        dictionary[i] = base + (random.nextInt(3) == 0
            ? random.nextInt(1 << 20)
            : i);
      }
      final boolean cyclic = random.nextBoolean();
      final int[] keys = new int[count];
      final int[] slots = new int[count];
      int slot = 0;
      boolean usable = true;
      for (int i = 0; i < count; i++) {
        keys[i] = cyclic
            ? dictionary[i % unique]
            : dictionary[random.nextInt(unique)];
        slot += 1 + random.nextInt(2);
        if (slot > 1_023) {
          usable = false;
          break;
        }
        slots[i] = slot;
      }
      if (!usable) {
        continue;
      }
      final byte[] scratch = new byte[1 + 255 * 4 + 2 + 128 + 1_024 + 64];
      final int legacyLength =
          PathNodeKeyRegion.encode(keys, slots, count, scratch, new int[256], new byte[1_200], new long[16]);
      if (legacyLength <= 0) {
        continue;
      }
      final byte[] legacy = Arrays.copyOf(scratch, legacyLength);
      final byte[] compactBuffer = new byte[legacyLength];
      final int compactLength = PathNodeKeyRegion.compact(legacy, legacyLength, compactBuffer);
      if (compactLength <= 0) {
        continue;
      }
      final byte[] compact = Arrays.copyOf(compactBuffer, compactLength);
      assertTrue(compactLength < legacyLength, "the compact form is only kept when it is smaller");
      assertEquals(legacyLength, PathNodeKeyRegion.expandedSize(compact, compactLength),
          "the compact header must say exactly how much it expands to");
      final byte[] expanded = new byte[legacyLength];
      PathNodeKeyRegion.expand(compact, compactLength, expanded);
      assertArrayEquals(legacy, expanded, "expansion must reproduce the random-access layout byte for byte");
      for (int i = 0; i < count; i++) {
        assertEquals(keys[i], PathNodeKeyRegion.pathNodeKeyForSlot(expanded, slots[i]),
            "slot " + slots[i] + " must look up to the key it was encoded with");
      }
      accepted++;
      if (cyclic) {
        cyclicAccepted++;
      }
    }
    assertTrue(accepted > 100, "the fuzz must actually accept shapes, accepted " + accepted);
    assertTrue(cyclicAccepted > 20, "and among them the record-shaped ones the run encoder exists for");
  }

  @Test
  @DisplayName("a page of records pays a fraction of what the random-access layout costs")
  void aPageOfRecordsPaysAFractionOfTheRandomAccessLayout() {
    // One record's worth of fields repeated: the dictionary ids walk 0..fields-1 and start over, which
    // is a constant-stride run per record and one step back.
    final int fields = 40;
    final int records = 25;
    final int count = fields * records;
    final int[] keys = new int[count];
    final int[] slots = new int[count];
    for (int i = 0; i < count; i++) {
      keys[i] = 900 + (i % fields);
      slots[i] = i;
    }
    final byte[] scratch = new byte[1 + 255 * 4 + 2 + 128 + 1_024 + 64];
    final int legacyLength =
        PathNodeKeyRegion.encode(keys, slots, count, scratch, new int[256], new byte[1_200], new long[16]);
    assertTrue(legacyLength > 0, "the fixture must produce a column");
    final byte[] legacy = Arrays.copyOf(scratch, legacyLength);
    final byte[] compactBuffer = new byte[legacyLength];
    final int compactLength = PathNodeKeyRegion.compact(legacy, legacyLength, compactBuffer);
    assertTrue(compactLength > 0, "the compact form must be reachable for a record-shaped page");
    // The 128-byte slot bitmap survives compaction and dominates what is left, so the bar is set on
    // what compaction can actually reach: the dictionary and the id lane together. Measured on this
    // fixture they fall from 1,163 bytes to 150 — a frame-of-reference dictionary at one byte a key
    // and two runs per record — so a fifth is a bar the encoder clears with room and a regression in
    // either half would break.
    final int variablePartLegacy = legacyLength - 128;
    final int variablePartCompact = compactLength - 128;
    assertTrue(variablePartCompact * 5 < variablePartLegacy,
        "dictionary plus id lane must fall below a fifth — " + variablePartCompact + " vs " + variablePartLegacy);
    assertTrue(compactLength * 2 < legacyLength,
        "and the column as a whole must more than halve — " + compactLength + " vs " + legacyLength);
    final byte[] expanded = new byte[legacyLength];
    PathNodeKeyRegion.expand(compact(compactBuffer, compactLength), compactLength, expanded);
    assertArrayEquals(legacy, expanded, "and still expand back exactly");
  }

  @Test
  @DisplayName("a dictionary of consecutive path ids is stored as deltas, and then it compresses")
  void aConsecutiveDictionaryIsStoredAsDeltas() {
    // A page's distinct pathNodeKeys are usually a run of consecutive path ids. Stored as offsets from
    // the minimum that is a byte RAMP — every byte distinct, nothing for the body codec to match — so
    // the dictionary was the one part of the compact column that came out BIGGER after LZ77 (106 bytes
    // in, 110 out). Stored as deltas it is the same byte repeated.
    final int keys = 106;
    final int[] pathNodeKeys = new int[keys];
    final int[] slots = new int[keys];
    for (int i = 0; i < keys; i++) {
      pathNodeKeys[i] = 900 + i;
      slots[i] = i;
    }
    final byte[] scratch = new byte[1 + 255 * 4 + 2 + 128 + 1_024 + 64];
    final int legacyLength =
        PathNodeKeyRegion.encode(pathNodeKeys, slots, keys, scratch, new int[256], new byte[1_200], new long[16]);
    final byte[] legacy = Arrays.copyOf(scratch, legacyLength);
    final byte[] compactBuffer = new byte[legacyLength];
    final int compactLength = PathNodeKeyRegion.compact(legacy, legacyLength, compactBuffer);
    assertTrue(compactLength > 0, "the compact form must be reachable");

    // The dictionary slice sits behind a three-byte header and the four-byte minimum.
    final int numUnique = compactBuffer[2] & 0xFF;
    final int dictWidth = 1 << (compactBuffer[1] & 0x03);
    final int dictBytes = numUnique * dictWidth;
    assertEquals(keys, numUnique, "every key on this fixture is distinct");
    final int dictCompressed = lz77Size(compactBuffer, 7, dictBytes);
    assertTrue(dictCompressed * 8 < dictBytes, "a delta dictionary must compress at least eightfold — " + dictCompressed
        + " of " + dictBytes + " bytes; the offset form came out LARGER than its input");

    // And it still expands to exactly the keys it was built from.
    final byte[] expanded = new byte[legacyLength];
    PathNodeKeyRegion.expand(Arrays.copyOf(compactBuffer, compactLength), compactLength, expanded);
    assertArrayEquals(legacy, expanded, "the delta dictionary must rebuild the random-access layout exactly");
    for (int i = 0; i < keys; i++) {
      assertEquals(pathNodeKeys[i], PathNodeKeyRegion.pathNodeKeyForSlot(expanded, slots[i]), "slot " + slots[i]);
    }
  }

  /** What the page body's dominant codec makes of a slice of these bytes on their own. */
  private static int lz77Size(final byte[] bytes, final int offset, final int length) {
    final MemorySegment segment = Arena.ofAuto().allocate(length);
    MemorySegment.copy(bytes, offset, segment, ValueLayout.JAVA_BYTE, 0L, length);
    final byte[] out = new byte[SirixLZ77Codec.maxEncodedSize(length)];
    return SirixLZ77Codec.encode(segment, 0L, length, out, 0);
  }

  @Test
  @DisplayName("the kill switch keeps the random-access layout on the wire")
  void killSwitchKeepsTheRandomAccessLayout() {
    final ResourceConfiguration config = newConfig();

    PageKind.PATH_NODE_KEY_COLUMN_COMPACT = true;
    final byte[] compactWire = serializeRecordShapedPage(config);
    PageKind.PATH_NODE_KEY_COLUMN_COMPACT = false;
    final byte[] legacyWire = serializeRecordShapedPage(config);

    assertNotEquals(hex(legacyWire), hex(compactWire), "the switch must actually change the bytes");
    assertTrue(compactWire.length < legacyWire.length,
        "and the compact form must be the smaller — " + compactWire.length + " vs " + legacyWire.length);

    // Both forms have to read back to the same records, since a resource can hold pages of each.
    PageKind.PATH_NODE_KEY_COLUMN_COMPACT = true;
    final byte[][] fromCompact = roundTripSlots(config, compactWire);
    PageKind.PATH_NODE_KEY_COLUMN_COMPACT = false;
    final byte[][] fromLegacy = roundTripSlots(config, legacyWire);
    assertEquals(fromLegacy.length, fromCompact.length);
    for (int slot = 0; slot < fromLegacy.length; slot++) {
      assertArrayEquals(fromLegacy[slot], fromCompact[slot], "slot " + slot);
    }
    // And a page written compact must read with the switch off, because the page states its own form.
    PageKind.PATH_NODE_KEY_COLUMN_COMPACT = false;
    final byte[][] crossRead = roundTripSlots(config, compactWire);
    for (int slot = 0; slot < fromLegacy.length; slot++) {
      assertArrayEquals(fromLegacy[slot], crossRead[slot], "slot " + slot + " read with the switch off");
    }
  }

  @Test
  @DisplayName("a non-positive pathNodeKey stays inline, because the column cannot report it back")
  void aNonPositivePathNodeKeyStaysInline() {
    // The reason, stated as an assertion rather than a comment: the column's lookup has one value for
    // "no entry" and it is the same one a stored -1 would return.
    final byte[] scratch = new byte[1 + 255 * 4 + 2 + 128 + 1_024 + 64];
    final int length = PathNodeKeyRegion.encode(new int[] {-1, -1}, new int[] {3, 9}, 2, scratch, new int[256],
        new byte[1_200], new long[16]);
    assertTrue(length > 0);
    assertEquals(-1, PathNodeKeyRegion.pathNodeKeyForSlot(scratch, 3),
        "a stored -1 is indistinguishable from the lookup's absent sentinel");
    assertEquals(-1, PathNodeKeyRegion.pathNodeKeyForSlot(scratch, 4), "which is what an absent slot returns");

    // So the writer keeps such slots inline, and the page round-trips. Before the compaction made the
    // column pay on this shape, the same page simply never took the column and the defect stayed
    // latent; the assertion is on the outcome either way.
    PageKind.PATH_NODE_KEY_COLUMN_COMPACT = true;
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    try {
      for (int i = 0; i < 300; i++) {
        writeNumber(page, i, 200 + (i % 20), -1L, Integer.valueOf(i * 5));
      }
      back = roundTrip(config, page);
      for (int slot = 0; slot < 300; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), back.getSlotAsByteArray(slot), "slot " + slot);
      }
    } finally {
      if (back != null) {
        back.close();
      }
      page.close();
    }
  }

  // ──────────────────────────────────────────────────────────────── helpers

  private static byte[] compact(final byte[] buffer, final int length) {
    return Arrays.copyOf(buffer, length);
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

  private byte[][] roundTripSlots(final ResourceConfiguration config, final byte[] wire) {
    final BytesIn<?> source = Bytes.elasticOffHeapByteBuffer().write(wire).bytesForRead();
    source.readByte();
    final KeyValueLeafPage back =
        (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
    try {
      final byte[][] slots = new byte[RECORD_SHAPED_SLOTS][];
      for (int slot = 0; slot < RECORD_SHAPED_SLOTS; slot++) {
        slots[slot] = back.getSlotAsByteArray(slot);
      }
      return slots;
    } finally {
      back.close();
    }
  }

  /** Slots the record-shaped fixture populates. */
  private static final int RECORD_SHAPED_SLOTS = 400;

  /** Field count of the record-shaped fixture, repeated across the page like a real record load. */
  private static final int RECORD_SHAPED_FIELDS = 25;

  private void fillRecordShaped(final KeyValueLeafPage page) {
    for (int i = 0; i < RECORD_SHAPED_SLOTS; i++) {
      writeNumber(page, i, 200 + (i % RECORD_SHAPED_FIELDS), 900 + (i % RECORD_SHAPED_FIELDS), Integer.valueOf(i * 3));
    }
  }

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder("pathNodeKeyColumnCompaction").buildPathSummary(true).build();
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
      final Number value) {
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0,
        0, 0L, value, HASH_FN, (byte[]) null);
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
