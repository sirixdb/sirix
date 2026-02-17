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
 * Partial key mapping for HOT (Height Optimized Trie) nodes.
 * 
 * <p>
 * Maps between full keys and their partial key representation using discriminative bit extraction.
 * Supports single-mask and multi-mask layouts.
 * </p>
 * 
 * <p>
 * <b>Reference Implementation:</b> {@code SingleMaskPartialKeyMapping.hpp}
 * </p>
 * 
 * <p>
 * <b>Key Operations:</b>
 * </p>
 * <ul>
 * <li>{@code extractMask(key)}: Extract partial key from full key using PEXT</li>
 * <li>{@code getPrefixBitsMask(bit)}: Get mask for all bits before a given bit</li>
 * <li>{@code getMaskFor(bit)}: Get mask for a specific discriminative bit</li>
 * </ul>
 * 
 * <p>
 * <b>Layout Types:</b>
 * </p>
 * <ul>
 * <li><b>SingleMask:</b> 9 bytes (1 byte offset + 8 byte mask), for bits within 8-byte window</li>
 * <li><b>MultiMask:</b> Variable, for bits spanning multiple byte windows</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * @see SparsePartialKeys
 * @see DiscriminativeBitComputer
 */
public final class PartialKeyMapping {

  /** Maximum bit span for single-mask layout (64 bits = 8 bytes). */
  private static final int MAX_SINGLE_MASK_BITS = 64;

  // ===== Common fields =====
  private final int mostSignificantBitIndex;
  private final int leastSignificantBitIndex;

  // ===== Single-mask layout =====
  private final int offsetInBytes;
  private final long successiveExtractionMask;

  // ===== Multi-mask layout (for spans > 8 bytes) =====
  private final boolean isMultiMask;
  private final byte[] extractionBytePositions;
  private final long[] extractionMasks;

  /**
   * Create a single-mask partial key mapping for a single discriminative bit.
   * 
   * @param discriminativeBitIndex the absolute bit index
   * @return new PartialKeyMapping
   */
  public static PartialKeyMapping forSingleBit(int discriminativeBitIndex) {
    int byteIndex = discriminativeBitIndex / 8;
    int bitInByte = discriminativeBitIndex % 8;

    // Offset: start of the 8-byte window containing this bit
    // For bits 0-63, offset is 0
    // For bits 64-127, offset is 8, etc.
    int offsetInBytes = (byteIndex / 8) * 8;

    // Position within the 8-byte window
    int relativeByteIndex = byteIndex - offsetInBytes;

    // Create extraction mask with the bit in correct position
    // The mask is built such that byte 0 is in bits 56-63, byte 7 in bits 0-7
    long mask = 1L << ((7 - relativeByteIndex) * 8 + (7 - bitInByte));

    return new PartialKeyMapping(discriminativeBitIndex, discriminativeBitIndex, offsetInBytes, mask);
  }

  /**
   * Create a partial key mapping by adding a discriminative bit to an existing mapping.
   * 
   * @param existing the existing mapping
   * @param newBitIndex the new discriminative bit to add
   * @return new PartialKeyMapping
   */
  public static PartialKeyMapping withAdditionalBit(@NonNull PartialKeyMapping existing, int newBitIndex) {
    int newMostSig = Math.min(existing.mostSignificantBitIndex, newBitIndex);
    int newLeastSig = Math.max(existing.leastSignificantBitIndex, newBitIndex);

    int byteSpan = (newLeastSig / 8) - (newMostSig / 8) + 1;

    if (byteSpan <= 8 && !existing.isMultiMask) {
      // Can still use single-mask layout
      // Offset must cover both the existing bits and the new bit
      int newOffset = (newMostSig / 8 / 8) * 8; // Round down to 8-byte boundary

      int newByteIndex = newBitIndex / 8;
      int newBitInByte = newBitIndex % 8;
      int relativeByteIndex = newByteIndex - newOffset;

      // Build new mask bit
      long additionalMask = 1L << ((7 - relativeByteIndex) * 8 + (7 - newBitInByte));

      // Adjust existing mask if offset changed
      long adjustedExisting = existing.successiveExtractionMask;
      if (newOffset != existing.offsetInBytes) {
        int shiftAmount = (existing.offsetInBytes - newOffset) * 8;
        if (shiftAmount > 0) {
          adjustedExisting = adjustedExisting << shiftAmount;
        } else {
          adjustedExisting = adjustedExisting >>> (-shiftAmount);
        }
      }

      return new PartialKeyMapping(newMostSig, newLeastSig, newOffset, adjustedExisting | additionalMask);
    } else {
      // Need multi-mask layout
      return createMultiMask(existing, newBitIndex, newMostSig, newLeastSig);
    }
  }

  private static PartialKeyMapping createMultiMask(PartialKeyMapping existing, int newBitIndex, int mostSig,
      int leastSig) {
    // For simplicity, create a 2-window multi-mask
    // In production, this would handle up to 4 windows for 32-bit partial keys
    byte[] positions = new byte[2];
    long[] masks = new long[2];

    positions[0] = (byte) (mostSig / 8);
    positions[1] = (byte) (leastSig / 8);

    // Build masks for each window
    masks[0] = existing.successiveExtractionMask;
    masks[1] = getSuccessiveMaskForBit(0, newBitIndex % 8);

    return new PartialKeyMapping(mostSig, leastSig, positions, masks);
  }

  // Single-mask constructor
  private PartialKeyMapping(int mostSigBit, int leastSigBit, int offset, long mask) {
    this.mostSignificantBitIndex = mostSigBit;
    this.leastSignificantBitIndex = leastSigBit;
    this.offsetInBytes = offset;
    this.successiveExtractionMask = mask;
    this.isMultiMask = false;
    this.extractionBytePositions = null;
    this.extractionMasks = null;
  }

  // Multi-mask constructor
  private PartialKeyMapping(int mostSigBit, int leastSigBit, byte[] positions, long[] masks) {
    this.mostSignificantBitIndex = mostSigBit;
    this.leastSignificantBitIndex = leastSigBit;
    this.offsetInBytes = positions[0];
    this.successiveExtractionMask = 0;
    this.isMultiMask = true;
    this.extractionBytePositions = positions;
    this.extractionMasks = masks;
  }

  /**
   * Extract partial key from a full key.
   * 
   * <p>
   * <b>Reference:</b> SingleMaskPartialKeyMapping.hpp extractMask()
   * </p>
   * 
   * <p>
   * Uses {@code Long.compress()} which maps to PEXT instruction on x86-64.
   * </p>
   * 
   * @param keyBytes the full key bytes
   * @return the extracted partial key
   */
  public int extractMask(@NonNull byte[] keyBytes) {
    if (isMultiMask) {
      return extractMaskMulti(keyBytes);
    }

    // Build 64-bit value from key bytes at offset
    long keyValue = 0;
    int end = Math.min(offsetInBytes + 8, keyBytes.length);
    for (int i = offsetInBytes; i < end; i++) {
      keyValue |= ((long) (keyBytes[i] & 0xFF)) << ((7 - (i - offsetInBytes)) * 8);
    }

    // Use PEXT (parallel bit extract) via Long.compress
    return (int) Long.compress(keyValue, successiveExtractionMask);
  }

  private int extractMaskMulti(byte[] keyBytes) {
    int result = 0;
    int bitOffset = 0;

    for (int i = 0; i < extractionMasks.length; i++) {
      int pos = extractionBytePositions[i] & 0xFF;
      long keyValue = 0;
      int end = Math.min(pos + 8, keyBytes.length);
      for (int j = pos; j < end; j++) {
        keyValue |= ((long) (keyBytes[j] & 0xFF)) << ((7 - (j - pos)) * 8);
      }

      int extracted = (int) Long.compress(keyValue, extractionMasks[i]);
      int numBits = Long.bitCount(extractionMasks[i]);
      result |= (extracted << bitOffset);
      bitOffset += numBits;
    }

    return result;
  }

  /**
   * Get a prefix bits mask for entries before a given discriminative bit.
   * 
   * <p>
   * <b>Reference:</b> SingleMaskPartialKeyMapping.hpp getPrefixBitsMask()
   * </p>
   * 
   * @param bitIndex the discriminative bit position
   * @return mask with prefix bits set
   */
  public int getPrefixBitsMask(int bitIndex) {
    if (bitIndex <= mostSignificantBitIndex) {
      return 0;
    }

    int bytePos = bitIndex / 8;
    int bitInByte = bitIndex % 8;
    int relativeByte = bytePos - offsetInBytes;

    if (relativeByte < 0 || relativeByte >= 8) {
      return getAllMaskBits();
    }

    // Create mask: all 1s for bytes before, partial for current byte
    long prefixMask = 0;
    for (int i = 0; i < relativeByte; i++) {
      prefixMask |= (0xFFL << ((7 - i) * 8));
    }
    // Add partial byte mask (bits before bitInByte)
    if (bitInByte > 0) {
      long partialByte = (0xFF << (8 - bitInByte)) & 0xFF;
      prefixMask |= (partialByte << ((7 - relativeByte) * 8));
    }

    return (int) Long.compress(prefixMask, successiveExtractionMask);
  }

  /**
   * Get mask for a specific discriminative bit position.
   * 
   * @param bitIndex the absolute bit position
   * @return mask with that bit's position in the partial key
   */
  public int getMaskFor(int bitIndex) {
    int bytePos = bitIndex / 8;
    int bitInByte = bitIndex % 8;
    long singleBitMask = getSuccessiveMaskForBit(bytePos - offsetInBytes, bitInByte);
    return (int) Long.compress(singleBitMask, successiveExtractionMask);
  }

  /**
   * Get mask with all discriminative bits set.
   * 
   * @return mask with all bits set
   */
  public int getAllMaskBits() {
    return (int) Long.compress(successiveExtractionMask, successiveExtractionMask);
  }

  /**
   * Get the mask for the highest (most significant) bit.
   * 
   * @return mask with only the MSB set
   */
  public int getMaskForHighestBit() {
    // The highest bit in the extraction mask corresponds to bit 0 in the result
    long highestBit = Long.highestOneBit(successiveExtractionMask);
    return (int) Long.compress(highestBit, successiveExtractionMask);
  }

  /**
   * Get the number of discriminative bits.
   * 
   * @return count of set bits
   */
  public int getNumberBitsUsed() {
    return Long.bitCount(successiveExtractionMask);
  }

  /**
   * Get the most significant discriminative bit index.
   * 
   * @return the most significant bit index
   */
  public int getMostSignificantBitIndex() {
    return mostSignificantBitIndex;
  }

  /**
   * Get the least significant discriminative bit index.
   * 
   * @return the least significant bit index
   */
  public int getLeastSignificantBitIndex() {
    return leastSignificantBitIndex;
  }

  /**
   * Get byte offset for this mapping.
   * 
   * @return offset in bytes
   */
  public int getOffsetInBytes() {
    return offsetInBytes;
  }

  /**
   * Get the extraction mask.
   * 
   * @return the 64-bit extraction mask
   */
  public long getExtractionMask() {
    return successiveExtractionMask;
  }

  /**
   * Check if this is a multi-mask mapping.
   * 
   * @return true if multi-mask
   */
  public boolean isMultiMask() {
    return isMultiMask;
  }

  /**
   * Create successive mask for a bit at given byte and bit position.
   * 
   * <p>
   * <b>Reference:</b> SingleMaskPartialKeyMapping.hpp getSuccessiveMaskForBit()
   * </p>
   * 
   * @param maskRelativeBytePosition byte position relative to mask offset
   * @param byteRelativeBitPosition bit position within byte (0=MSB, 7=LSB)
   * @return 64-bit mask with that bit set
   */
  private static long getSuccessiveMaskForBit(int maskRelativeBytePosition, int byteRelativeBitPosition) {
    if (maskRelativeBytePosition < 0 || maskRelativeBytePosition >= 8) {
      return 0;
    }
    // Reference formula: (maskRelativeBytePosition * 8) + 7 - byteRelativeBitPosition
    int bitIndex = (maskRelativeBytePosition * 8) + 7 - byteRelativeBitPosition;
    return 1L << bitIndex;
  }
}

