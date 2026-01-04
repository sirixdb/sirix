/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link SlotOffsetCodec}.
 * <p>
 * Tests cover:
 * - Empty pages
 * - Single slot
 * - Fully populated pages
 * - Sparse pages
 * - Large offsets (requiring 16+ bits)
 * - Round-trip serialization/deserialization
 * - Bit-packing edge cases
 */
public final class SlotOffsetCodecTest {

  @Test
  public void testEmptyPage() {
    // All slots empty (-1)
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, -1);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Empty page should decode to all -1");
  }

  @Test
  public void testSingleSlot() {
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    slotOffsets[0] = 0;

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, 0);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Single slot at index 0 should round-trip correctly");
  }

  @Test
  public void testSingleSlotMiddle() {
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    slotOffsets[512] = 1000;

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, 512);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Single slot in middle should round-trip correctly");
  }

  @Test
  public void testFullyPopulated() {
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    int offset = 0;
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      slotOffsets[i] = offset;
      offset += 50; // Each record ~50 bytes
    }

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, Constants.NDP_NODE_COUNT - 1);

    long compressedSize = sink.writePosition();

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Fully populated page should round-trip correctly");

    // Verify compression - should be much smaller than 4096 bytes
    int rawSize = Constants.NDP_NODE_COUNT * 4; // 4096 bytes
    System.out.printf("Full page compression: raw=%d, compressed=%d, ratio=%.1f%%%n",
                      rawSize,
                      compressedSize,
                      (100.0 * compressedSize / rawSize));
  }

  @Test
  public void testSparsePageRandom() {
    Random random = new Random(42); // Fixed seed for reproducibility
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    // Populate ~10% of slots
    int offset = 0;
    int lastSlot = -1;
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      if (random.nextDouble() < 0.1) {
        slotOffsets[i] = offset;
        offset += random.nextInt(100) + 10;
        lastSlot = i;
      }
    }

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, lastSlot);

    long compressedSize = sink.writePosition();

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Sparse page should round-trip correctly");

    int rawSize = Constants.NDP_NODE_COUNT * 4;
    System.out.printf("Sparse page compression: raw=%d, compressed=%d, ratio=%.1f%%%n",
                      rawSize,
                      compressedSize,
                      (100.0 * compressedSize / rawSize));
  }

  @Test
  public void testLargeOffsets() {
    // Test with offsets requiring 16+ bits
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    slotOffsets[0] = 0;
    slotOffsets[1] = 65536;  // 2^16
    slotOffsets[2] = 131072; // 2^17
    slotOffsets[3] = 200000;

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, 3);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Large offsets should round-trip correctly");
  }

  @Test
  public void testMaxIntOffsets() {
    // Test with maximum integer offsets
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    slotOffsets[0] = 0;
    slotOffsets[1] = Integer.MAX_VALUE / 2;
    slotOffsets[2] = Integer.MAX_VALUE;

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, 2);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Max int offsets should round-trip correctly");
  }

  @Test
  public void testConsecutiveSlots() {
    // Slots 0-99 with sequential offsets
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    for (int i = 0; i < 100; i++) {
      slotOffsets[i] = i * 32;
    }

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, 99);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Consecutive slots should round-trip correctly");
  }

  @Test
  public void testOffsetsWithGaps() {
    // Test offsets with varying gaps between them (all still monotonic)
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    // Offsets with varying gaps
    slotOffsets[0] = 0;
    slotOffsets[1] = 10;    // Small gap
    slotOffsets[2] = 1000;  // Large gap
    slotOffsets[3] = 1001;  // Tiny gap

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, 3);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Varying gap offsets should round-trip");
  }

  @Test
  public void testBitPackingWithVariousBitWidths() {
    // Test bit widths that are commonly used (1-16 bits covers typical offset deltas)
    int[] bitWidths = { 1, 2, 3, 4, 5, 7, 8, 15, 16 };

    for (int bitWidth : bitWidths) {
      // Create values that require exactly bitWidth bits
      int maxValue = (1 << bitWidth) - 1;
      int[] values = new int[100];
      Random random = new Random(bitWidth);
      for (int i = 0; i < values.length; i++) {
        values[i] = random.nextInt(maxValue + 1);
      }

      BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      SlotOffsetCodec.writeBitPacked(sink, values, bitWidth);

      BytesIn<?> source = sink.bytesForRead();
      int[] decoded = SlotOffsetCodec.readBitPacked(source, values.length, bitWidth);

      assertArrayEquals(values, decoded, "Bit-packing with " + bitWidth + " bits should round-trip correctly");
    }
  }

  @Test
  public void testBitPackingWith32Bits() {
    // Special test for 32-bit values which need careful handling
    int[] values = { 0, 1, Integer.MAX_VALUE / 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE };

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.writeBitPacked(sink, values, 32);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.readBitPacked(source, values.length, 32);

    assertArrayEquals(values, decoded, "32-bit packing should round-trip correctly");
  }

  @Test
  public void testBitPackingEdgeCaseZeroValues() {
    int[] values = new int[100];
    Arrays.fill(values, 0);

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.writeBitPacked(sink, values, 1);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.readBitPacked(source, values.length, 1);

    assertArrayEquals(values, decoded, "All-zero values should round-trip");
  }

  @Test
  public void testBitPackingEmptyArray() {
    int[] values = new int[0];

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.writeBitPacked(sink, values, 8);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.readBitPacked(source, 0, 8);

    assertArrayEquals(values, decoded, "Empty array should round-trip");
  }

  @Test
  public void testCompressionRatioRealistic() {
    // Simulate realistic page with varying record sizes
    Random random = new Random(12345);
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    int offset = 0;
    int lastSlot = -1;

    // Populate 80% of slots with realistic record sizes (30-200 bytes)
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      if (random.nextDouble() < 0.8) {
        slotOffsets[i] = offset;
        offset += 30 + random.nextInt(170); // 30-200 byte records
        lastSlot = i;
      }
    }

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, lastSlot);

    long compressedSize = sink.writePosition();
    int rawSize = Constants.NDP_NODE_COUNT * 4;

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertArrayEquals(slotOffsets, decoded, "Realistic page should round-trip correctly");

    double ratio = (100.0 * compressedSize / rawSize);
    System.out.printf("Realistic page (80%% full): raw=%d, compressed=%d, ratio=%.1f%%, savings=%.1f%%%n",
                      rawSize,
                      compressedSize,
                      ratio,
                      100 - ratio);

    // Expect at least 50% compression
    assert compressedSize < rawSize * 0.5 : "Expected at least 50% compression but got " + ratio + "%";
  }

  @Test
  public void testNullSinkThrows() {
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    assertThrows(NullPointerException.class, () -> SlotOffsetCodec.encode(null, slotOffsets, -1));
  }

  @Test
  public void testNullOffsetsThrows() {
    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    assertThrows(NullPointerException.class, () -> SlotOffsetCodec.encode(sink, null, -1));
  }

  @Test
  public void testWrongArrayLengthThrows() {
    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    int[] wrongSize = new int[100]; // Not 1024
    assertThrows(IllegalArgumentException.class, () -> SlotOffsetCodec.encode(sink, wrongSize, -1));
  }

  @Test
  public void testNullSourceThrows() {
    assertThrows(NullPointerException.class, () -> SlotOffsetCodec.decode(null));
  }

  @Test
  public void testFirstSlotOnly() {
    // Edge case: only slot 0 is populated
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    slotOffsets[0] = 0;

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, 0);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    assertEquals(0, decoded[0]);
    for (int i = 1; i < Constants.NDP_NODE_COUNT; i++) {
      assertEquals(-1, decoded[i], "Slot " + i + " should be -1");
    }
  }

  @Test
  public void testLastSlotOnly() {
    // Edge case: only the last slot is populated
    int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    slotOffsets[Constants.NDP_NODE_COUNT - 1] = 65535;

    BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    SlotOffsetCodec.encode(sink, slotOffsets, Constants.NDP_NODE_COUNT - 1);

    BytesIn<?> source = sink.bytesForRead();
    int[] decoded = SlotOffsetCodec.decode(source);

    for (int i = 0; i < Constants.NDP_NODE_COUNT - 1; i++) {
      assertEquals(-1, decoded[i], "Slot " + i + " should be -1");
    }
    assertEquals(65535, decoded[Constants.NDP_NODE_COUNT - 1]);
  }
}



