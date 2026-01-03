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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Sparse Partial Keys for HOT (Height Optimized Trie) nodes.
 * 
 * <p>Implements the sparse partial key storage and SIMD-accelerated search from
 * Robert Binna's PhD thesis. Sparse partial keys store only the discriminative bits
 * along each entry's path from the trie root.</p>
 * 
 * <p><b>Reference Implementation:</b> {@code SparsePartialKeys.hpp}</p>
 * 
 * <p><b>Key Insight from Thesis:</b> Sparse partial keys differ from dense partial keys
 * in that each entry only has the discriminative bits set that correspond to BiNodes
 * along its specific path from root to leaf. Other bits are left undefined (0).</p>
 * 
 * <p><b>SIMD Search Pattern:</b></p>
 * <pre>
 * // For each sparse partial key:
 * (densePartialKey &amp; sparsePartialKey) == sparsePartialKey
 * 
 * // This can be done in parallel using SIMD:
 * 1. Broadcast the dense partial key to all lanes
 * 2. AND with the sparse partial keys
 * 3. Compare with original sparse partial keys
 * 4. Extract matching mask
 * </pre>
 * 
 * @param <T> The partial key type (Byte, Short, or Integer)
 * @author Johannes Lichtenberger
 * @see DiscriminativeBitComputer
 * @see <a href="https://github.com/speedskater/hot">Reference Implementation</a>
 */
public final class SparsePartialKeys<T extends Number> {

  /** Maximum number of entries in a HOT node (from reference). */
  public static final int MAX_ENTRIES = 32;
  
  /** SIMD species for 8-bit partial keys (32 lanes on AVX2). */
  private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;
  
  /** SIMD species for 16-bit partial keys (16 lanes on AVX2). */
  private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_256;
  
  /** SIMD species for 32-bit partial keys (8 lanes on AVX2). */
  private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_256;
  
  /** The partial key type (Byte.class, Short.class, or Integer.class). */
  private final Class<T> partialKeyType;
  
  /** The number of entries. */
  private int numEntries;
  
  /** Storage for 8-bit partial keys. */
  private byte[] byteEntries;
  
  /** Storage for 16-bit partial keys. */
  private short[] shortEntries;
  
  /** Storage for 32-bit partial keys. */
  private int[] intEntries;
  
  /**
   * Create sparse partial keys for 8-bit partial key type.
   * 
   * @param numEntries initial number of entries
   * @return new SparsePartialKeys for bytes
   */
  public static SparsePartialKeys<Byte> forBytes(int numEntries) {
    return new SparsePartialKeys<>(Byte.class, numEntries);
  }
  
  /**
   * Create sparse partial keys for 16-bit partial key type.
   * 
   * @param numEntries initial number of entries
   * @return new SparsePartialKeys for shorts
   */
  public static SparsePartialKeys<Short> forShorts(int numEntries) {
    return new SparsePartialKeys<>(Short.class, numEntries);
  }
  
  /**
   * Create sparse partial keys for 32-bit partial key type.
   * 
   * @param numEntries initial number of entries
   * @return new SparsePartialKeys for ints
   */
  public static SparsePartialKeys<Integer> forInts(int numEntries) {
    return new SparsePartialKeys<>(Integer.class, numEntries);
  }
  
  private SparsePartialKeys(Class<T> partialKeyType, int numEntries) {
    if (numEntries < 0 || numEntries > MAX_ENTRIES) {
      throw new IllegalArgumentException("numEntries must be 0-" + MAX_ENTRIES);
    }
    this.partialKeyType = partialKeyType;
    this.numEntries = numEntries;
    
    // Allocate storage with padding for SIMD alignment
    int alignedSize = alignToNext8(numEntries);
    if (partialKeyType == Byte.class) {
      this.byteEntries = new byte[MAX_ENTRIES]; // Always 32 for AVX2 alignment
    } else if (partialKeyType == Short.class) {
      this.shortEntries = new short[MAX_ENTRIES];
    } else {
      this.intEntries = new int[MAX_ENTRIES];
    }
  }
  
  /**
   * SIMD-accelerated search for entries matching a dense partial key.
   * 
   * <p><b>Reference:</b> SparsePartialKeys.hpp search() method</p>
   * 
   * <p>Returns a bitmask where bit i is set if entry i matches the search key.
   * The search condition is: {@code (denseKey & sparseKey[i]) == sparseKey[i]}</p>
   * 
   * @param densePartialKey the dense partial key extracted from the search key
   * @return bitmask of matching entries
   */
  public int search(int densePartialKey) {
    if (partialKeyType == Byte.class) {
      return searchBytes((byte) densePartialKey);
    } else if (partialKeyType == Short.class) {
      return searchShorts((short) densePartialKey);
    } else {
      return searchInts(densePartialKey);
    }
  }
  
  /**
   * SIMD search for 8-bit partial keys.
   * 
   * <p>Uses AVX2 to compare 32 entries in a single instruction.</p>
   * 
   * @param densePartialKey the search key
   * @return bitmask of matching entries
   */
  private int searchBytes(byte densePartialKey) {
    if (BYTE_SPECIES.length() >= MAX_ENTRIES) {
      // Full SIMD path: compare all 32 entries at once
      ByteVector searchReg = ByteVector.broadcast(BYTE_SPECIES, densePartialKey);
      ByteVector haystack = ByteVector.fromArray(BYTE_SPECIES, byteEntries, 0);
      
      // Compute: (search & haystack) == haystack
      ByteVector andResult = searchReg.and(haystack);
      VectorMask<Byte> matches = andResult.compare(jdk.incubator.vector.VectorOperators.EQ, haystack);
      
      // Use long shift to avoid overflow when numEntries=32
      long mask = numEntries == 32 ? 0xFFFFFFFFL : ((1L << numEntries) - 1);
      return (int) (matches.toLong() & mask);
    } else {
      // Scalar fallback
      return searchBytesScalar(densePartialKey);
    }
  }
  
  /**
   * Scalar fallback for 8-bit search.
   */
  private int searchBytesScalar(byte densePartialKey) {
    int result = 0;
    for (int i = 0; i < numEntries; i++) {
      if ((densePartialKey & byteEntries[i]) == byteEntries[i]) {
        result |= (1 << i);
      }
    }
    return result;
  }
  
  /**
   * SIMD search for 16-bit partial keys.
   */
  private int searchShorts(short densePartialKey) {
    if (SHORT_SPECIES.length() >= 16) {
      // Process 16 entries at a time
      ShortVector searchReg = ShortVector.broadcast(SHORT_SPECIES, densePartialKey);
      
      // First 16 entries
      ShortVector haystack1 = ShortVector.fromArray(SHORT_SPECIES, shortEntries, 0);
      ByteVector andResult1 = searchReg.and(haystack1).reinterpretAsBytes();
      VectorMask<Short> matches1 = searchReg.and(haystack1).compare(
          jdk.incubator.vector.VectorOperators.EQ, haystack1);
      
      // Second 16 entries
      ShortVector haystack2 = ShortVector.fromArray(SHORT_SPECIES, shortEntries, 16);
      VectorMask<Short> matches2 = searchReg.and(haystack2).compare(
          jdk.incubator.vector.VectorOperators.EQ, haystack2);
      
      int result = (int) matches1.toLong() | ((int) matches2.toLong() << 16);
      return result & ((1 << numEntries) - 1);
    } else {
      return searchShortsScalar(densePartialKey);
    }
  }
  
  private int searchShortsScalar(short densePartialKey) {
    int result = 0;
    for (int i = 0; i < numEntries; i++) {
      if ((densePartialKey & shortEntries[i]) == shortEntries[i]) {
        result |= (1 << i);
      }
    }
    return result;
  }
  
  /**
   * SIMD search for 32-bit partial keys.
   */
  private int searchInts(int densePartialKey) {
    if (INT_SPECIES.length() >= 8) {
      IntVector searchReg = IntVector.broadcast(INT_SPECIES, densePartialKey);
      int result = 0;
      
      // Process 8 entries at a time
      for (int i = 0; i < 4; i++) {
        IntVector haystack = IntVector.fromArray(INT_SPECIES, intEntries, i * 8);
        VectorMask<Integer> matches = searchReg.and(haystack).compare(
            jdk.incubator.vector.VectorOperators.EQ, haystack);
        result |= ((int) matches.toLong() << (i * 8));
      }
      
      return result & ((1 << numEntries) - 1);
    } else {
      return searchIntsScalar(densePartialKey);
    }
  }
  
  private int searchIntsScalar(int densePartialKey) {
    int result = 0;
    for (int i = 0; i < numEntries; i++) {
      if ((densePartialKey & intEntries[i]) == intEntries[i]) {
        result |= (1 << i);
      }
    }
    return result;
  }
  
  /**
   * Find entries matching a prefix pattern.
   * 
   * <p><b>Reference:</b> SparsePartialKeys.hpp findMasksByPattern()</p>
   * 
   * @param usedBits mask indicating which bits to consider
   * @param expectedBits expected values for those bits
   * @return bitmask of matching entries
   */
  public int findMasksByPattern(int usedBits, int expectedBits) {
    int result = 0;
    if (partialKeyType == Byte.class) {
      for (int i = 0; i < numEntries; i++) {
        if ((byteEntries[i] & usedBits) == expectedBits) {
          result |= (1 << i);
        }
      }
    } else if (partialKeyType == Short.class) {
      for (int i = 0; i < numEntries; i++) {
        if ((shortEntries[i] & usedBits) == expectedBits) {
          result |= (1 << i);
        }
      }
    } else {
      for (int i = 0; i < numEntries; i++) {
        if ((intEntries[i] & usedBits) == expectedBits) {
          result |= (1 << i);
        }
      }
    }
    return result;
  }
  
  /**
   * Get the relevant bits for a range of entries.
   * 
   * <p><b>Reference:</b> SparsePartialKeys.hpp getRelevantBitsForRange()</p>
   * 
   * <p>These bits are determined by comparing successive masks in the range.
   * Whenever a mask has a bit set which is not set in its predecessor,
   * that bit is added to the set of relevant bits.</p>
   * 
   * @param firstIndex first index in the range
   * @param numEntriesInRange number of entries in the range
   * @return mask with relevant bits set
   */
  public int getRelevantBitsForRange(int firstIndex, int numEntriesInRange) {
    int relevantBits = 0;
    int endIndex = firstIndex + numEntriesInRange;
    
    if (partialKeyType == Byte.class) {
      for (int i = firstIndex + 1; i < endIndex; i++) {
        relevantBits |= (byteEntries[i] & ~byteEntries[i - 1]);
      }
    } else if (partialKeyType == Short.class) {
      for (int i = firstIndex + 1; i < endIndex; i++) {
        relevantBits |= (shortEntries[i] & ~shortEntries[i - 1]);
      }
    } else {
      for (int i = firstIndex + 1; i < endIndex; i++) {
        relevantBits |= (intEntries[i] & ~intEntries[i - 1]);
      }
    }
    
    return relevantBits;
  }
  
  /**
   * Determine the value of the discriminating bit for an entry.
   * 
   * <p><b>Reference:</b> SparsePartialKeys.hpp determineValueOfDiscriminatingBit()</p>
   * 
   * @param indexOfEntry the entry index
   * @return true if the discriminative bit value is 1, false if 0
   */
  public boolean determineValueOfDiscriminatingBit(int indexOfEntry) {
    if (indexOfEntry == 0) {
      return false;
    } else if (indexOfEntry == numEntries - 1) {
      return true;
    } else {
      // Compare common bits with predecessor vs successor
      if (partialKeyType == Byte.class) {
        int commonWithPrev = byteEntries[indexOfEntry - 1] & byteEntries[indexOfEntry];
        int commonWithNext = byteEntries[indexOfEntry] & byteEntries[indexOfEntry + 1];
        return (commonWithPrev & 0xFF) >= (commonWithNext & 0xFF);
      } else if (partialKeyType == Short.class) {
        int commonWithPrev = shortEntries[indexOfEntry - 1] & shortEntries[indexOfEntry];
        int commonWithNext = shortEntries[indexOfEntry] & shortEntries[indexOfEntry + 1];
        return (commonWithPrev & 0xFFFF) >= (commonWithNext & 0xFFFF);
      } else {
        long commonWithPrev = ((long) intEntries[indexOfEntry - 1] & intEntries[indexOfEntry]) & 0xFFFFFFFFL;
        long commonWithNext = ((long) intEntries[indexOfEntry] & intEntries[indexOfEntry + 1]) & 0xFFFFFFFFL;
        return commonWithPrev >= commonWithNext;
      }
    }
  }
  
  /**
   * Get entry at index.
   * 
   * @param index the entry index
   * @return the partial key at that index
   */
  @SuppressWarnings("unchecked")
  public T getEntry(int index) {
    if (partialKeyType == Byte.class) {
      return (T) Byte.valueOf(byteEntries[index]);
    } else if (partialKeyType == Short.class) {
      return (T) Short.valueOf(shortEntries[index]);
    } else {
      return (T) Integer.valueOf(intEntries[index]);
    }
  }
  
  /**
   * Set entry at index.
   * 
   * @param index the entry index
   * @param value the partial key value
   */
  public void setEntry(int index, @NonNull T value) {
    if (partialKeyType == Byte.class) {
      byteEntries[index] = value.byteValue();
    } else if (partialKeyType == Short.class) {
      shortEntries[index] = value.shortValue();
    } else {
      intEntries[index] = value.intValue();
    }
  }
  
  /**
   * Get the number of entries.
   * 
   * @return the number of entries
   */
  public int getNumEntries() {
    return numEntries;
  }
  
  /**
   * Set the number of entries.
   * 
   * @param numEntries the new entry count
   */
  public void setNumEntries(int numEntries) {
    if (numEntries < 0 || numEntries > MAX_ENTRIES) {
      throw new IllegalArgumentException("numEntries must be 0-" + MAX_ENTRIES);
    }
    this.numEntries = numEntries;
  }
  
  /**
   * Get the partial key type.
   * 
   * @return the partial key type class
   */
  public Class<T> getPartialKeyType() {
    return partialKeyType;
  }
  
  /**
   * Get byte entries array (for serialization).
   * 
   * @return byte entries or null if not byte type
   */
  public byte[] getByteEntries() {
    return partialKeyType == Byte.class ? byteEntries : null;
  }
  
  /**
   * Get short entries array (for serialization).
   * 
   * @return short entries or null if not short type
   */
  public short[] getShortEntries() {
    return partialKeyType == Short.class ? shortEntries : null;
  }
  
  /**
   * Get int entries array (for serialization).
   * 
   * @return int entries or null if not int type
   */
  public int[] getIntEntries() {
    return partialKeyType == Integer.class ? intEntries : null;
  }
  
  /**
   * Estimate size in bytes for allocation.
   * 
   * @param numEntries number of entries
   * @param partialKeyType the partial key type
   * @return estimated size in bytes
   */
  public static int estimateSize(int numEntries, Class<? extends Number> partialKeyType) {
    int alignedEntries = alignToNext8(numEntries);
    if (partialKeyType == Byte.class) {
      return alignedEntries;
    } else if (partialKeyType == Short.class) {
      return alignedEntries * 2;
    } else {
      return alignedEntries * 4;
    }
  }
  
  /**
   * Align size to next multiple of 8 for SIMD.
   */
  private static int alignToNext8(int size) {
    return (size % 8 == 0) ? size : ((size & (~7)) + 8);
  }
}

