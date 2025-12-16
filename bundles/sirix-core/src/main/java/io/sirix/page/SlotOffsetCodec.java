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

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.settings.Constants;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Codec for compressing slot offset arrays using bit-packing.
 * 
 * <p>This compression technique is inspired by best practices from DuckDB, RocksDB,
 * and Apache Parquet. It achieves significant space savings compared to raw int[1024] arrays:</p>
 * <ul>
 *   <li>Sparse pages (10% full): ~95% savings</li>
 *   <li>Realistic pages (80% full): ~50% savings</li>
 *   <li>Full pages: ~50% savings</li>
 * </ul>
 * 
 * <h2>Format</h2>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                    Compressed Slot Offsets                       │
 * ├─────────────────────────────────────────────────────────────────┤
 * │ Presence Bitmap   │ Bit Width │ Bit-Packed Offsets              │
 * │ (BitSet ~128B)    │ (1 byte)  │ (N×bitWidth bits)               │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * <h2>Algorithm</h2>
 * <ol>
 *   <li>Build a presence bitmap indicating which slots are populated (offset >= 0)</li>
 *   <li>Extract populated offsets in slot order</li>
 *   <li>Determine minimum bit width needed to represent max offset</li>
 *   <li>Bit-pack all offsets using the computed bit width</li>
 * </ol>
 * 
 * <p>Note: Delta encoding is not used because slot offsets are not guaranteed to be 
 * monotonically increasing when iterated in slot order.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class SlotOffsetCodec {

  private SlotOffsetCodec() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Encode slot offsets using bit-packing.
   * 
   * <p>Format: [presence bitmap][bitWidth (1B)][bit-packed offsets]</p>
   * 
   * @param sink the output to write compressed data to
   * @param slotOffsets the slot offset array (length must be Constants.NDP_NODE_COUNT)
   * @param lastSlotIndex unused, kept for API compatibility
   * @throws NullPointerException if sink or slotOffsets is null
   * @throws IllegalArgumentException if slotOffsets.length != Constants.NDP_NODE_COUNT
   */
  public static void encode(final BytesOut<?> sink, final int[] slotOffsets, final int lastSlotIndex) {
    if (sink == null) {
      throw new NullPointerException("sink must not be null");
    }
    if (slotOffsets == null) {
      throw new NullPointerException("slotOffsets must not be null");
    }
    if (slotOffsets.length != Constants.NDP_NODE_COUNT) {
      throw new IllegalArgumentException(
          "slotOffsets.length must be " + Constants.NDP_NODE_COUNT + " but was " + slotOffsets.length);
    }

    // 1. Build presence bitmap by scanning all slots
    // Note: lastSlotIndex tracks the slot with largest OFFSET, not highest slot NUMBER
    // So we must scan all slots to find all populated ones
    final BitSet presence = new BitSet(Constants.NDP_NODE_COUNT);
    int populatedCount = 0;
    
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      if (slotOffsets[i] >= 0) {
        presence.set(i);
        populatedCount++;
      }
    }
    
    SerializationType.serializeBitSet(sink, presence);

    if (populatedCount == 0) {
      return; // Empty page - just bitmap is sufficient
    }

    // 2. Extract populated offsets in slot order and find max
    final int[] populatedOffsets = new int[populatedCount];
    int idx = 0;
    int maxOffset = 0;
    for (int i = presence.nextSetBit(0); i >= 0; i = presence.nextSetBit(i + 1)) {
      final int offset = slotOffsets[i];
      populatedOffsets[idx++] = offset;
      maxOffset = Math.max(maxOffset, offset);
    }

    // 3. Determine bit width from max offset (not deltas - offsets aren't monotonic)
    int bitWidth = 32 - Integer.numberOfLeadingZeros(maxOffset);
    if (bitWidth == 0) {
      bitWidth = 1; // Minimum 1 bit
    }

    sink.writeByte((byte) bitWidth);

    // 4. Bit-pack all offsets directly
    writeBitPacked(sink, populatedOffsets, bitWidth);
  }

  /**
   * Decode compressed slot offsets back to int[1024] array.
   * 
   * <p>Empty slots are marked with -1.</p>
   * 
   * @param source the input to read compressed data from
   * @return the decoded slot offset array of length Constants.NDP_NODE_COUNT
   * @throws NullPointerException if source is null
   */
  public static int[] decode(final BytesIn<?> source) {
    if (source == null) {
      throw new NullPointerException("source must not be null");
    }

    final int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);

    // 1. Read presence bitmap
    final BitSet presence = SerializationType.deserializeBitSet(source);
    final int populatedCount = presence.cardinality();

    if (populatedCount == 0) {
      return slotOffsets; // Empty page
    }

    // 2. Read bit width and bit-packed offsets (direct, not deltas)
    final int bitWidth = source.readByte() & 0xFF;
    final int[] populatedOffsets = readBitPacked(source, populatedCount, bitWidth);

    // 3. Place back into sparse array at correct slot positions
    int idx = 0;
    for (int i = presence.nextSetBit(0); i >= 0; i = presence.nextSetBit(i + 1)) {
      slotOffsets[i] = populatedOffsets[idx++];
    }

    return slotOffsets;
  }

  /**
   * Pack integers using exactly bitWidth bits each.
   * 
   * <p>Values are packed in little-endian bit order within each byte.</p>
   * 
   * @param sink the output to write packed data to
   * @param values the integer values to pack
   * @param bitWidth the number of bits to use per value (1-32)
   */
  static void writeBitPacked(final BytesOut<?> sink, final int[] values, final int bitWidth) {
    if (values.length == 0) {
      return;
    }

    // Calculate total bytes needed
    final int totalBits = values.length * bitWidth;
    final int totalBytes = (totalBits + 7) / 8;

    final byte[] packed = new byte[totalBytes];
    int bitPos = 0;

    for (int value : values) {
      // Write 'bitWidth' bits of value starting at bitPos
      final int bytePos = bitPos / 8;
      final int bitOffset = bitPos % 8;

      // Handle value spanning multiple bytes (up to 5 bytes for 32-bit values)
      final long shifted = ((long) value) << bitOffset;
      final int bytesNeeded = (bitOffset + bitWidth + 7) / 8;

      for (int b = 0; b < bytesNeeded && bytePos + b < packed.length; b++) {
        packed[bytePos + b] |= (byte) (shifted >>> (b * 8));
      }
      bitPos += bitWidth;
    }

    sink.write(packed);
  }

  /**
   * Unpack integers from bitWidth-bit packed format.
   * 
   * @param source the input to read packed data from
   * @param count the number of integers to unpack
   * @param bitWidth the number of bits per value (1-32)
   * @return the unpacked integer values
   */
  static int[] readBitPacked(final BytesIn<?> source, final int count, final int bitWidth) {
    if (count == 0) {
      return new int[0];
    }

    final int totalBits = count * bitWidth;
    final int totalBytes = (totalBits + 7) / 8;

    final byte[] packed = new byte[totalBytes];
    source.read(packed);

    final int[] values = new int[count];
    int bitPos = 0;
    final int mask = (bitWidth == 32) ? -1 : (1 << bitWidth) - 1;

    for (int i = 0; i < count; i++) {
      final int bytePos = bitPos / 8;
      final int bitOffset = bitPos % 8;

      // Read up to 5 bytes to handle 32-bit values spanning byte boundaries
      long accumulated = 0;
      for (int b = 0; b < 5 && bytePos + b < packed.length; b++) {
        accumulated |= ((long) (packed[bytePos + b] & 0xFF)) << (b * 8);
      }

      values[i] = (int) ((accumulated >>> bitOffset) & mask);
      bitPos += bitWidth;
    }

    return values;
  }
}
