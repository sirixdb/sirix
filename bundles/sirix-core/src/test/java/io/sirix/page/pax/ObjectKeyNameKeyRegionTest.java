/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link ObjectKeyNameKeyRegion}'s bitmap expansion: the round-trip, and the corrupt
 * shape where the bitmap disagrees with the declared count — which must be a {@code -1} refusal,
 * never an out-of-bounds throw, because callers size their output to the declared count.
 */
@DisplayName("ObjectKeyNameKeyRegion")
final class ObjectKeyNameKeyRegionTest {

  /** Slots spread across several bitmap words, in bitmap order. */
  private static final int[] SLOTS = {3, 64, 130, 700, 1023};

  private static final int[] NAME_KEYS = {7, 7, 9, 7, 11};

  @Test
  @DisplayName("decodeSlotsInto: round-trip in bitmap order")
  void decodeSlotsRoundTrip() {
    final byte[] wire = ObjectKeyNameKeyRegion.encode(NAME_KEYS, SLOTS, SLOTS.length);
    assertNotNull(wire);
    final MemorySegment seg = PaxTestSegments.of(wire);
    final int[] out = new int[SLOTS.length];
    assertEquals(SLOTS.length, ObjectKeyNameKeyRegion.decodeSlotsInto(seg, out));
    assertArrayEquals(SLOTS, out);
  }

  @Test
  @DisplayName("decodeSlotsInto: a bitmap popcount above the declared count answers -1, not a throw")
  void bitmapPopcountAboveDeclaredCountDeclines() {
    final byte[] wire = ObjectKeyNameKeyRegion.encode(NAME_KEYS, SLOTS, SLOTS.length);
    assertNotNull(wire);
    // Understate okCount while leaving all five bitmap bits set. Callers size out[] to the
    // declared count, so the extra set bits used to overrun the array before the count check ran.
    final int numUnique = wire[0] & 0xFF;
    final int countOffset = 1 + numUnique * 4;
    final int declaredCount = 3;
    wire[countOffset] = (byte) declaredCount;
    wire[countOffset + 1] = 0;
    final MemorySegment seg = PaxTestSegments.of(wire);
    assertEquals(declaredCount, ObjectKeyNameKeyRegion.count(seg));
    final int[] out = new int[declaredCount];
    assertEquals(-1, ObjectKeyNameKeyRegion.decodeSlotsInto(seg, out));
  }

  @Test
  @DisplayName("decodeSlotsInto: an undersized output array answers -1")
  void undersizedOutputDeclines() {
    final byte[] wire = ObjectKeyNameKeyRegion.encode(NAME_KEYS, SLOTS, SLOTS.length);
    assertNotNull(wire);
    final MemorySegment seg = PaxTestSegments.of(wire);
    final int[] out = new int[SLOTS.length - 1];
    assertEquals(-1, ObjectKeyNameKeyRegion.decodeSlotsInto(seg, out));
  }

  @Test
  @DisplayName("encodeInto is wire-identical across large/small/large scratch reuse")
  void encodeIntoIsWireIdenticalAcrossScratchReuse() {
    final int[] fullSlots = new int[1024];
    final int[] fullNameKeys = new int[1024];
    for (int i = 0; i < fullSlots.length; i++) {
      fullSlots[i] = i;
      fullNameKeys[i] = 10_000 + i % 105;
    }
    final byte[] expectedFull = ObjectKeyNameKeyRegion.encode(fullNameKeys, fullSlots, fullSlots.length);
    final byte[] expectedSmall = ObjectKeyNameKeyRegion.encode(NAME_KEYS, SLOTS, SLOTS.length);
    final byte[] reusable = new byte[ObjectKeyNameKeyRegion.maxEncodedSize(1024)];

    int length = ObjectKeyNameKeyRegion.encodeInto(fullNameKeys, fullSlots, fullSlots.length, reusable);
    assertEquals(expectedFull.length, length);
    assertArrayEquals(expectedFull, Arrays.copyOf(reusable, length));

    Arrays.fill(reusable, (byte) 0x5A);
    length = ObjectKeyNameKeyRegion.encodeInto(NAME_KEYS, SLOTS, SLOTS.length, reusable);
    assertEquals(expectedSmall.length, length);
    assertArrayEquals(expectedSmall, Arrays.copyOf(reusable, length),
        "a smaller page must not retain bitmap or dictionary bytes from the full page");

    Arrays.fill(reusable, (byte) 0xA5);
    length = ObjectKeyNameKeyRegion.encodeInto(fullNameKeys, fullSlots, fullSlots.length, reusable);
    assertArrayEquals(expectedFull, Arrays.copyOf(reusable, length));
  }

  @Test
  @DisplayName("encodeInto retains the exact V1 little-endian wire layout")
  void encodeIntoMatchesFixedWireFixture() {
    final byte[] expected = new byte[141];
    expected[0] = 2; // dictionary size
    expected[1] = 0x04;
    expected[2] = 0x03;
    expected[3] = 0x02;
    expected[4] = 0x01;
    expected[5] = (byte) 0xFE;
    expected[6] = (byte) 0xFF;
    expected[7] = (byte) 0xFF;
    expected[8] = (byte) 0xFF;
    expected[9] = 2; // OBJECT_KEY count, little-endian short
    expected[11] = 1; // slot 0
    expected[19] = 2; // slot 65: bit 1 of bitmap word 1
    expected[140] = 1; // dictionary ids 0, 1
    final byte[] reusable = new byte[expected.length];

    final int length = ObjectKeyNameKeyRegion.encodeInto(new int[] {0x01020304, -2}, new int[] {0, 65}, 2, reusable);

    assertEquals(expected.length, length);
    assertArrayEquals(expected, reusable);
  }

  @Test
  @DisplayName("encodeInto preserves the 255-entry dictionary ceiling and validates storage")
  void encodeIntoRefusalAndValidation() {
    final int[] slots = new int[256];
    final int[] distinctNameKeys = new int[256];
    for (int i = 0; i < slots.length; i++) {
      slots[i] = i;
      distinctNameKeys[i] = i;
    }
    final byte[] reusable = new byte[ObjectKeyNameKeyRegion.maxEncodedSize(1024)];
    assertEquals(ObjectKeyNameKeyRegion.ENCODE_FAILED,
        ObjectKeyNameKeyRegion.encodeInto(distinctNameKeys, slots, slots.length, reusable));
    assertNull(ObjectKeyNameKeyRegion.encode(distinctNameKeys, slots, slots.length));

    final byte[] expected = ObjectKeyNameKeyRegion.encode(NAME_KEYS, SLOTS, SLOTS.length);
    assertThrows(IllegalArgumentException.class,
        () -> ObjectKeyNameKeyRegion.encodeInto(NAME_KEYS, SLOTS, SLOTS.length, new byte[expected.length - 1]));
    assertThrows(IllegalArgumentException.class,
        () -> ObjectKeyNameKeyRegion.encodeInto(new int[] {7}, new int[] {1024}, 1, reusable));

    final byte[] empty = ObjectKeyNameKeyRegion.encode(new int[0], new int[0], 0);
    assertNotNull(empty, "the legacy encoder publishes an empty, zero-bitmap column");
    assertEquals(ObjectKeyNameKeyRegion.maxEncodedSize(0), empty.length);
  }
}
