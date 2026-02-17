/*
 * Copyright (c) 2024, Sirix Contributors
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

package io.sirix.index.hot;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Utility class for computing discriminative bits between keys.
 * 
 * <p>
 * Implements the discriminative bit computation algorithm from Robert Binna's PhD thesis on Height
 * Optimized Tries (HOT). The discriminative bit is the first bit position where two keys differ,
 * used to determine trie structure.
 * </p>
 * 
 * <p>
 * <b>Reference Implementation:</b> {@code DiscriminativeBit.hpp} lines 52-55
 * </p>
 * 
 * <p>
 * <b>Algorithm:</b> XOR the bytes at each position, find the first non-zero result, then use
 * count-leading-zeros to find the bit position within that byte.
 * </p>
 * 
 * <p>
 * <b>Performance:</b> Branchless computation for predictable latency, suitable for high-performance
 * financial systems.
 * </p>
 * 
 * @author Johannes Lichtenberger
 * @see <a href="https://github.com/speedskater/hot">Reference HOT Implementation</a>
 */
public final class DiscriminativeBitComputer {

  /** Private constructor to prevent instantiation. */
  private DiscriminativeBitComputer() {
    throw new AssertionError("Utility class - do not instantiate");
  }

  /**
   * Compute the first differing bit position between two keys.
   * 
   * <p>
   * This is the discriminative bit that separates left and right subtrees in a HOT trie. The bit
   * position is 0-indexed from the MSB of the first byte.
   * </p>
   * 
   * <p>
   * <b>Reference:</b> DiscriminativeBit.hpp line 52-55:
   * {@code return __builtin_clz(existingByte ^ newKeyByte) - 24;}
   * </p>
   * 
   * @param key1 first key (typically max key in left subtree)
   * @param key2 second key (typically min key in right subtree)
   * @return bit position (0-indexed from MSB), or -1 if keys are identical
   * @throws NullPointerException if either key is null
   */
  public static int computeDifferingBit(@NonNull byte[] key1, @NonNull byte[] key2) {
    if (key1.length == 0 && key2.length == 0) {
      return -1; // Both empty, no difference
    }
    if (key1.length == 0 || key2.length == 0) {
      return 0; // One empty, differ at first bit
    }

    int minLen = Math.min(key1.length, key2.length);

    // Process 8 bytes at a time using long comparison (cache-friendly)
    int i = 0;
    for (; i + 8 <= minLen; i += 8) {
      long left = getLongBE(key1, i);
      long right = getLongBE(key2, i);
      if (left != right) {
        // numberOfLeadingZeros on long XOR gives bit position directly
        return i * 8 + Long.numberOfLeadingZeros(left ^ right);
      }
    }

    // Handle remaining bytes one at a time
    // Reference: DiscriminativeBit.hpp line 52-55 uses __builtin_clz(xor) - 24
    for (; i < minLen; i++) {
      int diff = (key1[i] ^ key2[i]) & 0xFF;
      if (diff != 0) {
        // For a byte in lower 8 bits of int, clz returns 24-31
        // Subtract 24 to get bit position within byte (0-7, 0=MSB)
        return i * 8 + (Integer.numberOfLeadingZeros(diff) - 24);
      }
    }

    // Keys share common prefix, differ in length
    // The discriminative bit is at position minLen * 8 (first bit of the longer key's suffix)
    if (key1.length != key2.length) {
      return minLen * 8;
    }

    // Keys are identical
    return -1;
  }

  /**
   * Check if a specific bit is set in a key.
   * 
   * <p>
   * <b>Reference:</b> Algorithms.hpp line 87-89:
   * {@code (existingRawKey[getByteIndex(mAbsoluteBitIndex)] & (0b10000000 >> bitPositionInByte(mAbsoluteBitIndex))) > 0}
   * </p>
   * 
   * @param key the key to check
   * @param absoluteBitIndex the absolute bit index (0 = MSB of first byte)
   * @return true if the bit is set (1), false otherwise (0)
   */
  public static boolean isBitSet(@NonNull byte[] key, int absoluteBitIndex) {
    if (absoluteBitIndex < 0) {
      return false;
    }
    int byteIndex = absoluteBitIndex / 8;
    if (byteIndex >= key.length) {
      return false; // Bit is beyond key length, treated as 0
    }
    int bitInByte = absoluteBitIndex % 8;
    // 0x80 >> bitInByte creates mask: 0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01
    return (key[byteIndex] & (0x80 >> bitInByte)) != 0;
  }

  /**
   * Compute the discriminative bit mask for a set of sorted keys.
   * 
   * <p>
   * This mask has bits set at all positions where at least two keys differ. Used for SpanNode and
   * MultiNode creation where multiple discriminative bits are needed.
   * </p>
   * 
   * @param sortedKeys array of keys in sorted order
   * @param startBytePos starting byte position for mask computation
   * @param maxBytes maximum number of bytes to consider (up to 8 for 64-bit mask)
   * @return 64-bit mask with discriminative bit positions set
   */
  public static long computeDiscriminativeMask(@NonNull byte[][] sortedKeys, int startBytePos, int maxBytes) {
    if (sortedKeys.length < 2) {
      return 0L;
    }

    long mask = 0L;
    int endBytePos = Math.min(startBytePos + maxBytes, 8); // Max 8 bytes for 64-bit mask

    // XOR fold: accumulate all differing bits across adjacent key pairs
    for (int keyIdx = 0; keyIdx < sortedKeys.length - 1; keyIdx++) {
      byte[] key1 = sortedKeys[keyIdx];
      byte[] key2 = sortedKeys[keyIdx + 1];

      for (int bytePos = startBytePos; bytePos < endBytePos; bytePos++) {
        int b1 = bytePos < key1.length
            ? (key1[bytePos] & 0xFF)
            : 0;
        int b2 = bytePos < key2.length
            ? (key2[bytePos] & 0xFF)
            : 0;
        int diff = b1 ^ b2;

        if (diff != 0) {
          // Place differing bits into the mask at the correct position
          // Byte 0 goes into bits 56-63, byte 1 into bits 48-55, etc.
          int shiftAmount = (7 - (bytePos - startBytePos)) * 8;
          mask |= ((long) diff) << shiftAmount;
        }
      }
    }

    return mask;
  }

  /**
   * Count the number of discriminative bits in a mask.
   * 
   * <p>
   * This determines the node type:
   * <ul>
   * <li>1 bit → BiNode (2 children)</li>
   * <li>2-4 bits → SpanNode (up to 16 children)</li>
   * <li>5+ bits → MultiNode (up to 256 children)</li>
   * </ul>
   * </p>
   * 
   * @param mask the discriminative bit mask
   * @return number of set bits
   */
  public static int countDiscriminativeBits(long mask) {
    return Long.bitCount(mask);
  }

  /**
   * Extract partial key from a full key using a discriminative bit mask.
   * 
   * <p>
   * <b>Reference:</b> SingleMaskPartialKeyMapping.hpp line 180-182 uses
   * {@code _pext_u64(inputMask, mSuccessiveExtractionMask)}
   * </p>
   * 
   * <p>
   * Java equivalent uses {@code Long.compress()} (Java 19+).
   * </p>
   * 
   * @param key the full key
   * @param mask the discriminative bit mask
   * @param startBytePos starting byte position in key
   * @return extracted partial key (bits packed into low positions)
   */
  public static int extractPartialKey(@NonNull byte[] key, long mask, int startBytePos) {
    // Build 64-bit value from key bytes
    long keyBits = 0L;
    for (int i = 0; i < 8 && (startBytePos + i) < key.length; i++) {
      keyBits |= ((long) (key[startBytePos + i] & 0xFF)) << ((7 - i) * 8);
    }

    // Use Long.compress (PEXT equivalent) to extract bits at mask positions
    return (int) Long.compress(keyBits, mask);
  }

  /**
   * Get the byte index for an absolute bit index.
   * 
   * @param absoluteBitIndex the absolute bit index
   * @return the byte index (absoluteBitIndex / 8)
   */
  public static int getByteIndex(int absoluteBitIndex) {
    return absoluteBitIndex / 8;
  }

  /**
   * Get the bit position within a byte for an absolute bit index.
   * 
   * @param absoluteBitIndex the absolute bit index
   * @return the bit position within the byte (0-7, 0=MSB)
   */
  public static int getBitPositionInByte(int absoluteBitIndex) {
    return absoluteBitIndex % 8;
  }

  /**
   * Read 8 bytes from array as big-endian long.
   * 
   * @param bytes the byte array
   * @param offset starting offset
   * @return 64-bit value in big-endian order
   */
  private static long getLongBE(byte[] bytes, int offset) {
    return ((long) (bytes[offset] & 0xFF) << 56) | ((long) (bytes[offset + 1] & 0xFF) << 48)
        | ((long) (bytes[offset + 2] & 0xFF) << 40) | ((long) (bytes[offset + 3] & 0xFF) << 32)
        | ((long) (bytes[offset + 4] & 0xFF) << 24) | ((long) (bytes[offset + 5] & 0xFF) << 16)
        | ((long) (bytes[offset + 6] & 0xFF) << 8) | ((long) (bytes[offset + 7] & 0xFF));
  }
}

