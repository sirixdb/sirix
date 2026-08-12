/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
