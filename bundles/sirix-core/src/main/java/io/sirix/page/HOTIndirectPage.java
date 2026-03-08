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

package io.sirix.page;

import io.sirix.index.hot.SparsePartialKeys;
import io.sirix.page.interfaces.Page;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * HOT (Height Optimized Trie) indirect page for cache-friendly secondary indexes.
 * 
 * <p>
 * Implements compound nodes that span multiple logical trie levels within a single page. Uses
 * discriminative bits and SIMD-optimized child lookup.
 * </p>
 * 
 * <p>
 * <b>Node Layout Types (from HOT dissertation):</b>
 * </p>
 * <ul>
 * <li>SINGLE_MASK: 9 bytes (initialBytePos + 64-bit mask), uses PEXT instruction</li>
 * <li>MULTI_MASK: Variable size, for bits spread across multiple bytes</li>
 * <li>POSITION_SEQUENCE: Variable size, explicit bit positions</li>
 * </ul>
 * 
 * <p>
 * <b>Node Types:</b>
 * </p>
 * <ul>
 * <li>SpanNode: 2-16 children, SIMD-searchable partial keys (includes 2-child nodes)</li>
 * <li>MultiNode: 17-32 children, SIMD-searchable partial keys</li>
 * </ul>
 * 
 * <p>
 * <b>Cache-Line Alignment (Reference: thesis section 4.3.2):</b>
 * </p>
 * <p>
 * For optimal cache performance on modern CPUs:
 * </p>
 * <ul>
 * <li>Hot data (bitMask, partialKeys) should be aligned to 64-byte cache lines</li>
 * <li>BiNode fits in a single cache line (2 children + mask = ~24 bytes)</li>
 * <li>SpanNode uses up to 2 cache lines (16 children + mask + keys)</li>
 * <li>Child references are accessed only after lookup, can be in separate lines</li>
 * </ul>
 * 
 * <p>
 * <b>SIMD Optimization (Reference: thesis section 4.3.3):</b>
 * </p>
 * <p>
 * SpanNode uses Long.compress() which maps to PEXT (Parallel Bit Extract) instruction on x86-64
 * with BMI2. Linear search of 2-16 partial keys is typically faster than binary search due to
 * better branch prediction and cache locality.
 * </p>
 * 
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see Page
 * @see <a href="https://github.com/speedskater/hot">Reference Implementation</a>
 */
public final class HOTIndirectPage implements Page {

  /** Sentinel for "not found" in child lookup. */
  public static final int NOT_FOUND = -1;

  /** Cache line size on modern x86-64 CPUs (64 bytes). */
  public static final int CACHE_LINE_SIZE = 64;

  /** Maximum children for a node (from reference: MAXIMUM_NUMBER_NODE_ENTRIES = 32). */
  public static final int MAX_NODE_ENTRIES = 32;

  /**
   * Node type enumeration.
   */
  public enum NodeType {
    /** 2-16 children, SIMD-searchable partial keys. */
    SPAN_NODE,
    /** 17-32 children, SIMD-searchable partial keys (same as SpanNode but higher fanout). */
    MULTI_NODE
  }

  /**
   * Layout type for discriminative bit storage.
   */
  public enum LayoutType {
    /** Single 64-bit mask with initial byte position. */
    SINGLE_MASK,
    /** Multiple 8-bit masks per byte. */
    MULTI_MASK,
    /** Explicit list of bit positions. */
    POSITION_SEQUENCE
  }

  /**
   * Compute the most significant discriminative bit index from a bitMask and initialBytePos.
   * The MSB is the bit closest to the start of the key (smallest absolute position).
   * In LE word layout: lowest byte group with a set bit, highest bit within that group.
   */
  public static short computeMostSignificantBitIndex(int initialBytePos, long bitMask) {
    if (bitMask == 0) {
      return (short) (initialBytePos * 8);
    }
    // Find the lowest set bit in the mask (lowest byte group = earliest key byte)
    final int lowestSetBit = Long.numberOfTrailingZeros(bitMask);
    final int byteGroup = lowestSetBit / 8;
    // Within that byte group, find the highest set bit (MSB of that key byte)
    final long byteBits = (bitMask >>> (byteGroup * 8)) & 0xFFL;
    final int highestBitInGroup = 63 - Long.numberOfLeadingZeros(byteBits); // 0-7
    // Convert LE word position to absolute MSB-first position
    return (short) ((initialBytePos + byteGroup) * 8 + (7 - highestBitInGroup));
  }

  // ===== Page identity =====
  private final long pageKey;
  private final int revision;
  private final int height; // Distance from leaves

  // ===== Node structure =====
  private final NodeType nodeType;
  private LayoutType layoutType; // Set in factory methods
  private int numChildren;

  // ===== Discriminative bits (layout-dependent) =====
  // SINGLE_MASK layout — widened to int to support keys longer than 255 bytes
  private int initialBytePos;
  private long bitMask;

  // Most significant discriminative bit index (absolute, MSB-first convention).
  // Matches C++ reference: PartialKeyMappingBase::mMostSignificantDiscriminativeBitIndex.
  // Tracked as min(all discriminative bit positions). Used by splitParentAndRecurse.
  private short mostSignificantBitIndex;

  // MULTI_MASK layout (C++ reference: MultiMaskPartialKeyMapping)
  // extractionPositions[i] = key byte index to gather for extraction byte i
  // extractionMasks = PEXT masks packed as 8 bytes per long (LE order)
  private byte[] extractionPositions;
  private long[] extractionMasks;
  private int numExtractionBytes;

  // POSITION_SEQUENCE layout
  private short[] bitPositions;

  // ===== Child data =====
  private int[] partialKeys; // Partial keys (PEXT-extracted disc bits per child)
  private @Nullable SparsePartialKeys<?> sparsePartialKeys; // SIMD-accelerated search
  private final PageReference[] childReferences; // References to child pages

  // ===== SIMD gather for MultiMask (vpshufb optimization) =====
  private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;
  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_256;

  // Transient SIMD state — lazily initialized from extractionPositions after deserialization.
  // VectorShuffle maps each extraction byte to its offset within the contiguous key load window.
  private transient VectorShuffle<Byte> gatherShuffle;
  private transient int gatherLoadOffset;   // min extraction position (start of load window)
  private transient boolean gatherFitsInVector; // true if span ≤ 32 bytes
  private transient boolean gatherInitialized;

  // ===== MultiNode direct index (256 bytes) =====
  private byte[] childIndex; // Maps byte value -> child slot

  /**
   * Determine the partial key width (in bytes) from the number of discriminative bits.
   * Matches C++ reference: uint8_t for ≤8 bits, uint16_t for ≤16, uint32_t for ≤32.
   *
   * @param bitMask the discriminative bit mask
   * @return 1 for byte, 2 for short, 4 for int
   */
  public static int determinePartialKeyWidth(long bitMask) {
    return determinePartialKeyWidthFromBitCount(Long.bitCount(bitMask));
  }

  /**
   * Determine partial key width from the total number of discriminative bits.
   * Matches C++ reference: uint8_t for ≤8, uint16_t for ≤16, uint32_t for ≤32.
   *
   * @param numDiscBits total number of discriminative bits
   * @return 1 for byte, 2 for short, 4 for int
   */
  public static int determinePartialKeyWidthFromBitCount(int numDiscBits) {
    if (numDiscBits <= 8) {
      return 1;
    } else if (numDiscBits <= 16) {
      return 2;
    } else {
      return 4;
    }
  }

  /**
   * Get the partial key width for this node, accounting for both SingleMask and MultiMask layouts.
   */
  private int getPartialKeyWidth() {
    if (layoutType == LayoutType.MULTI_MASK && extractionMasks != null) {
      int totalBits = 0;
      for (final long mask : extractionMasks) {
        totalBits += Long.bitCount(mask);
      }
      return determinePartialKeyWidthFromBitCount(totalBits);
    }
    return determinePartialKeyWidth(bitMask);
  }

  /**
   * Create a SparsePartialKeys instance of the appropriate width and populate it.
   */
  private static SparsePartialKeys<?> createSparsePartialKeys(int[] keys, int numEntries, int width) {
    if (width <= 1) {
      final SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(numEntries);
      for (int i = 0; i < numEntries; i++) {
        spk.setByteEntry(i, (byte) keys[i]);
      }
      return spk;
    } else if (width <= 2) {
      final SparsePartialKeys<Short> spk = SparsePartialKeys.forShorts(numEntries);
      for (int i = 0; i < numEntries; i++) {
        spk.setShortEntry(i, (short) keys[i]);
      }
      return spk;
    } else {
      final SparsePartialKeys<Integer> spk = SparsePartialKeys.forInts(numEntries);
      for (int i = 0; i < numEntries; i++) {
        spk.setIntEntry(i, keys[i]);
      }
      return spk;
    }
  }

  /**
   * Create a new BiNode with 2 children.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param discriminativeBitPos the bit position that distinguishes the two children
   * @param leftChild reference to left child (bit=0)
   * @param rightChild reference to right child (bit=1)
   * @return new BiNode
   */
  public static HOTIndirectPage createBiNode(long pageKey, int revision, int discriminativeBitPos,
      @NonNull PageReference leftChild, @NonNull PageReference rightChild) {
    return createBiNode(pageKey, revision, discriminativeBitPos, leftChild, rightChild, 0);
  }

  /**
   * Create a 2-child compound node (SpanNode) from a discriminative bit position.
   *
   * <p>In the C++ reference, BiNodes are ephemeral return values from split() that are
   * immediately integrated into the tree as proper compound nodes. This method follows
   * that pattern: it creates a SPAN_NODE with 2 children, complete with SparsePartialKeys
   * for PEXT-based routing. No special BiNode routing path is needed.</p>
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param discriminativeBitPos the absolute bit position that discriminates left/right
   * @param leftChild reference to left child (bit=0)
   * @param rightChild reference to right child (bit=1)
   * @param height the height of this node in the tree
   * @return new SpanNode with 2 children
   */
  public static HOTIndirectPage createBiNode(long pageKey, int revision, int discriminativeBitPos,
      @NonNull PageReference leftChild, @NonNull PageReference rightChild, int height) {
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, height, NodeType.SPAN_NODE, 2);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = discriminativeBitPos / 8;

    // Compute bit mask for PEXT extraction (LE word layout)
    int byteWithinWindow = (discriminativeBitPos / 8) - page.initialBytePos;
    int bitWithinByte = discriminativeBitPos % 8; // 0=MSB, 7=LSB
    int bitInWord = byteWithinWindow * 8 + (7 - bitWithinByte);
    page.bitMask = 1L << bitInWord;
    page.mostSignificantBitIndex = (short) discriminativeBitPos;

    page.partialKeys = new int[] {0, 1};
    page.sparsePartialKeys = createSparsePartialKeys(page.partialKeys, 2, 1);
    page.childReferences[0] = leftChild;
    page.childReferences[1] = rightChild;
    return page;
  }

  /**
   * Create a new SpanNode with 2-16 children.
   *
   * <p>
   * <b>Reference:</b> SpanNode uses SparsePartialKeys for SIMD-accelerated child lookup. The search
   * pattern is: {@code (denseKey & sparseKey) == sparseKey}
   * </p>
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param initialBytePos initial byte position for SINGLE_MASK
   * @param bitMask the 64-bit mask for extracting discriminative bits
   * @param partialKeys array of partial keys (extracted bits for each child)
   * @param children array of child references
   * @return new SpanNode
   */
  public static HOTIndirectPage createSpanNode(long pageKey, int revision, int initialBytePos, long bitMask,
      int[] partialKeys, PageReference[] children) {
    if (children.length < 2 || children.length > 16) {
      throw new IllegalArgumentException("SpanNode must have 2-16 children");
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, 0, NodeType.SPAN_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = initialBytePos;
    page.bitMask = bitMask;
    page.mostSignificantBitIndex = computeMostSignificantBitIndex(initialBytePos, bitMask);
    page.partialKeys = partialKeys.clone();
    page.sparsePartialKeys = createSparsePartialKeys(partialKeys, children.length, determinePartialKeyWidth(bitMask));
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  /**
   * Create a new MultiNode with 1-32 children using direct-byte childIndex array.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param discriminativeByte the byte position used for direct indexing
   * @param childIndex 256-byte array mapping byte values to child slots
   * @param children array of child references
   * @return new MultiNode
   */
  public static HOTIndirectPage createMultiNode(long pageKey, int revision, int discriminativeByte, byte[] childIndex,
      PageReference[] children) {
    if (children.length < 1 || children.length > MAX_NODE_ENTRIES) {
      throw new IllegalArgumentException("MultiNode must have 1-" + MAX_NODE_ENTRIES + " children, got: "
          + children.length);
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, 0, NodeType.MULTI_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = discriminativeByte;
    page.mostSignificantBitIndex = (short) (discriminativeByte * 8);
    page.childIndex = childIndex.clone();
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  /**
   * Private constructor for factory methods.
   */
  private HOTIndirectPage(long pageKey, int revision, int height, NodeType nodeType, int numChildren) {
    this.pageKey = pageKey;
    this.revision = revision;
    this.height = height;
    this.nodeType = nodeType;
    this.numChildren = numChildren;
    this.childReferences = new PageReference[numChildren];
    this.layoutType = LayoutType.SINGLE_MASK; // Default
  }

  /**
   * Copy constructor for COW operations.
   *
   * @param other the page to copy
   */
  public HOTIndirectPage(HOTIndirectPage other) {
    this.pageKey = other.pageKey;
    this.revision = other.revision;
    this.height = other.height;
    this.nodeType = other.nodeType;
    this.layoutType = other.layoutType;
    this.numChildren = other.numChildren;
    this.initialBytePos = other.initialBytePos;
    this.bitMask = other.bitMask;
    this.mostSignificantBitIndex = other.mostSignificantBitIndex;

    if (other.extractionPositions != null) {
      this.extractionPositions = other.extractionPositions.clone();
    }
    if (other.extractionMasks != null) {
      this.extractionMasks = other.extractionMasks.clone();
    }
    this.numExtractionBytes = other.numExtractionBytes;
    if (other.bitPositions != null) {
      this.bitPositions = other.bitPositions.clone();
    }
    if (other.partialKeys != null) {
      this.partialKeys = other.partialKeys.clone();
      this.sparsePartialKeys = createSparsePartialKeys(
          other.partialKeys, other.numChildren, other.getPartialKeyWidth());
    }
    if (other.childIndex != null) {
      this.childIndex = other.childIndex.clone();
    }

    this.childReferences = new PageReference[other.childReferences.length];
    for (int i = 0; i < other.numChildren; i++) {
      if (other.childReferences[i] != null) {
        this.childReferences[i] = new PageReference(other.childReferences[i]);
      }
    }
  }

  /**
   * Create a copy with an updated child reference (for COW propagation).
   *
   * @param childIndex the child index to update
   * @param newChildRef the new child reference
   * @return new page with updated child
   */
  public HOTIndirectPage copyWithUpdatedChild(int childIndex, PageReference newChildRef) {
    HOTIndirectPage copy = new HOTIndirectPage(this);
    copy.childReferences[childIndex] = newChildRef;
    return copy;
  }

  // ===== Child lookup methods =====

  /**
   * Find child index for the given key. Uses SIMD-optimized lookup based on node type.
   *
   * @param key the search key
   * @return child index, or NOT_FOUND (-1) if not found
   */
  public int findChildIndex(byte[] key) {
    return switch (nodeType) {
      case SPAN_NODE -> findChildSpanNode(key);
      case MULTI_NODE -> findChildMultiNode(key);
    };
  }

  /**
   * SpanNode lookup: Extract partial key and search in partial keys array.
   * 
   * <p>
   * <b>Reference:</b> SparsePartialKeys.hpp search() method
   * </p>
   * 
   * <p>
   * Uses SIMD-accelerated search when SparsePartialKeys is available. The search pattern is:
   * {@code (densePartialKey & sparsePartialKey) == sparsePartialKey}
   * </p>
   * 
   * <p>
   * This finds ALL entries that could match the search key based on the discriminative bits.
   * The HIGHEST (most-specific) match is selected — the entry with the most bits set in its
   * sparse partial key is the most specific match for this key's bit pattern.
   * </p>
   */
  private int findChildSpanNode(byte[] key) {
    final int densePartialKey;
    if (layoutType == LayoutType.MULTI_MASK) {
      densePartialKey = computeMultiMaskPartialKey(key);
    } else {
      if (initialBytePos >= key.length) {
        return 0;
      }
      long keyWord = getKeyWordAt(key, initialBytePos);
      densePartialKey = (int) Long.compress(keyWord, bitMask); // PEXT intrinsic
    }

    // Use SIMD-accelerated search if available
    if (sparsePartialKeys != null) {
      // SIMD search returns bitmask of all matching entries
      int matchMask = sparsePartialKeys.search(densePartialKey);
      if (matchMask == 0) {
        return NOT_FOUND;
      }
      // Return highest matching index (most-specific match per HOT paper)
      return 31 - Integer.numberOfLeadingZeros(matchMask);
    }

    // Fallback: Linear search — find the last (highest-index) matching entry
    int best = NOT_FOUND;
    for (int i = 0; i < numChildren; i++) {
      // Check: (denseKey & sparseKey[i]) == sparseKey[i]
      int sparseKey = partialKeys[i];
      if ((densePartialKey & sparseKey) == sparseKey) {
        best = i;
      }
    }
    return best;
  }

  /**
   * Compute partial key using MultiMask layout.
   *
   * <p>Dispatches to SIMD path (vpshufb gather) when extraction positions span ≤32 key bytes,
   * otherwise falls back to scalar. The SIMD path uses {@code ByteVector.rearrange()} which
   * compiles to vpshufb on x86 AVX2 — a single-cycle byte shuffle instruction.</p>
   *
   * <p>Matches C++ reference: MultiMaskPartialKeyMapping::extractMask() = mapInput() + extractMaskForMappedInput().</p>
   */
  private int computeMultiMaskPartialKey(byte[] key) {
    if (!gatherInitialized) {
      initGatherShuffle();
    }
    if (gatherFitsInVector) {
      return computeMultiMaskPartialKeySIMD(key);
    }
    return computeMultiMaskPartialKeyScalar(key);
  }

  /**
   * Initialize the SIMD gather shuffle from extraction positions.
   *
   * <p>Pre-computes a {@link VectorShuffle} that maps each extraction byte lane to the
   * offset of its source key byte within a contiguous load window. Lanes beyond
   * {@code numExtractionBytes} are zeroed via wrap-around to lane 0 (don't-care).</p>
   */
  private void initGatherShuffle() {
    gatherInitialized = true;
    if (extractionPositions == null || numExtractionBytes == 0) {
      gatherFitsInVector = false;
      return;
    }

    // Find min/max extraction positions to determine the load window
    int minPos = Integer.MAX_VALUE;
    int maxPos = Integer.MIN_VALUE;
    for (int i = 0; i < numExtractionBytes; i++) {
      final int pos = extractionPositions[i] & 0xFF;
      if (pos < minPos) minPos = pos;
      if (pos > maxPos) maxPos = pos;
    }

    final int span = maxPos - minPos + 1;
    if (span > BYTE_SPECIES.length()) {
      // Span exceeds vector width (>32 bytes for AVX2) — use scalar fallback
      gatherFitsInVector = false;
      return;
    }

    gatherLoadOffset = minPos;
    gatherFitsInVector = true;

    // Build shuffle indices: lane i → offset of extractionPositions[i] within load window
    // Unused lanes (>= numExtractionBytes) get index 0 (harmless — gathered value is don't-care)
    final int vectorLen = BYTE_SPECIES.length();
    final int[] shuffleIndices = new int[vectorLen];
    for (int i = 0; i < numExtractionBytes; i++) {
      shuffleIndices[i] = (extractionPositions[i] & 0xFF) - minPos;
    }
    // Unused lanes: point to byte 0 of vector (don't-care value, avoids out-of-range)
    for (int i = numExtractionBytes; i < vectorLen; i++) {
      shuffleIndices[i] = 0;
    }

    gatherShuffle = VectorShuffle.fromValues(BYTE_SPECIES, shuffleIndices);
  }

  /**
   * SIMD-accelerated MultiMask partial key extraction using vpshufb gather.
   *
   * <p>Algorithm:
   * <ol>
   *   <li>Load key bytes from [gatherLoadOffset .. gatherLoadOffset+31] into a ByteVector</li>
   *   <li>Rearrange using pre-computed gatherShuffle (vpshufb) — gathered bytes now in lanes 0..N-1</li>
   *   <li>Reinterpret as LongVector (4 longs for AVX2) — each long holds 8 gathered bytes</li>
   *   <li>Extract each long lane and apply Long.compress() (PEXT) with the extraction mask</li>
   *   <li>Concatenate PEXT results by bit-shifting</li>
   * </ol>
   * </p>
   */
  private int computeMultiMaskPartialKeySIMD(byte[] key) {
    final int vectorLen = BYTE_SPECIES.length();
    final int loadEnd = gatherLoadOffset + vectorLen;

    // Load key bytes into vector. If key is shorter than the load window, pad with zeros.
    final ByteVector keyVec;
    if (gatherLoadOffset + vectorLen <= key.length) {
      // Fast path: entire load window fits in key
      keyVec = ByteVector.fromArray(BYTE_SPECIES, key, gatherLoadOffset);
    } else {
      // Key is shorter than load window — copy available bytes into padded array
      final byte[] padded = new byte[vectorLen];
      final int available = Math.max(0, key.length - gatherLoadOffset);
      if (available > 0) {
        System.arraycopy(key, gatherLoadOffset, padded, 0, available);
      }
      keyVec = ByteVector.fromArray(BYTE_SPECIES, padded, 0);
    }

    // vpshufb: gather extraction bytes into contiguous lanes
    final ByteVector gathered = keyVec.rearrange(gatherShuffle);

    // Reinterpret gathered bytes as longs for PEXT
    final LongVector gatheredLongs = gathered.reinterpretAsLongs();

    // Apply PEXT per chunk and concatenate
    final int numChunks = extractionMasks.length;
    int result = 0;
    int shift = 0;
    for (int w = 0; w < numChunks; w++) {
      final long gatheredWord = gatheredLongs.lane(w);
      final int extracted = (int) Long.compress(gatheredWord, extractionMasks[w]);
      result |= extracted << shift;
      shift += Long.bitCount(extractionMasks[w]);
    }
    return result;
  }

  /**
   * Scalar fallback for MultiMask partial key extraction.
   * Used when extraction positions span more than 32 key bytes (exceeds AVX2 vector width).
   */
  private int computeMultiMaskPartialKeyScalar(byte[] key) {
    final int numChunks = extractionMasks.length;
    // Gather key bytes at extraction positions
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffset = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << (byteOffset * 8);
    }
    // Apply PEXT per chunk and combine
    int result = 0;
    int shift = 0;
    for (int w = 0; w < numChunks; w++) {
      final int extracted = (int) Long.compress(gathered[w], extractionMasks[w]);
      result |= extracted << shift;
      shift += Long.bitCount(extractionMasks[w]);
    }
    return result;
  }

  /**
   * MultiNode lookup: Direct byte indexing.
   */
  private int findChildMultiNode(byte[] key) {
    // If sparsePartialKeys is available, use SIMD-accelerated search (same as SpanNode)
    if (sparsePartialKeys != null) {
      return findChildSpanNode(key);
    }

    // Otherwise use direct byte indexing via childIndex array
    if (childIndex == null) {
      return 0; // Fallback
    }
    if (initialBytePos >= key.length) {
      return childIndex[0] & 0xFF; // Default to first entry
    }
    int keyByte = key[initialBytePos] & 0xFF;
    int index = childIndex[keyByte] & 0xFF;
    return index < numChildren
        ? index
        : NOT_FOUND;
  }

  /**
   * Extract up to 8 bytes from key starting at given position. Uses little-endian byte order for PEXT
   * compatibility — byte at {@code pos} maps to bits 0-7, byte at {@code pos+1} to bits 8-15, etc.
   *
   * <p><b>Important:</b> Both the construction path ({@code HOTTrieWriter.computeBitMaskForChildren},
   * {@code computePartialKey}) and this lookup path use the same LE byte layout. Within each byte,
   * MSB (bit 0) maps to position 7, LSB (bit 7) maps to position 0. {@link DiscriminativeBitComputer}
   * uses a separate BE convention for its own purposes — it is not involved in the PEXT lookup chain.</p>
   */
  private static long getKeyWordAt(byte[] key, int pos) {
    long result = 0;
    int end = Math.min(pos + 8, key.length);
    for (int i = pos; i < end; i++) {
      result |= ((long) (key[i] & 0xFF)) << ((i - pos) * 8);
    }
    return result;
  }

  /**
   * Get child reference at index.
   *
   * @param index the child index
   * @return the page reference
   */
  public PageReference getChildReference(int index) {
    Objects.checkIndex(index, numChildren);
    return childReferences[index];
  }

  /**
   * Set child reference at index.
   *
   * @param index the child index
   * @param ref the page reference
   */
  public void setChildReference(int index, PageReference ref) {
    Objects.checkIndex(index, numChildren);
    childReferences[index] = ref;
  }

  /**
   * Get number of children.
   *
   * @return number of children
   */
  public int getNumChildren() {
    return numChildren;
  }

  /**
   * Get node type.
   *
   * @return the node type
   */
  public NodeType getNodeType() {
    return nodeType;
  }

  /**
   * Get layout type.
   *
   * @return the layout type
   */
  public LayoutType getLayoutType() {
    return layoutType;
  }

  /**
   * Get page key.
   *
   * @return the page key
   */
  public long getPageKey() {
    return pageKey;
  }

  /**
   * Get revision.
   *
   * @return the revision
   */
  public int getRevision() {
    return revision;
  }

  /**
   * Get height (distance from leaves).
   *
   * @return the height
   */
  public int getHeight() {
    return height;
  }

  /**
   * Get initial byte position for discriminative bit extraction.
   *
   * @return the initial byte position
   */
  public int getInitialBytePos() {
    return initialBytePos;
  }

  /**
   * Get the 64-bit discriminative bit mask.
   *
   * @return the bit mask
   */
  public long getBitMask() {
    return bitMask;
  }

  /**
   * Get the most significant discriminative bit index (absolute, MSB-first convention).
   * Matches C++ reference: PartialKeyMappingBase::mMostSignificantDiscriminativeBitIndex.
   *
   * @return the most significant bit index
   */
  public short getMostSignificantBitIndex() {
    return mostSignificantBitIndex;
  }

  /**
   * Get partial key at index.
   *
   * @param index the index
   * @return the partial key value
   */
  public int getPartialKey(int index) {
    if (partialKeys == null || index < 0 || index >= partialKeys.length) {
      return 0;
    }
    return partialKeys[index];
  }

  /**
   * Get the partial keys array.
   *
   * @return a copy of the partial keys array, or empty array if null
   */
  public int[] getPartialKeys() {
    if (partialKeys == null) {
      return new int[numChildren];
    }
    return partialKeys.clone();
  }

  /**
   * Get the child index array for MultiNode serialization.
   *
   * @return a copy of the child index array, or null if not a MultiNode
   */
  public byte @Nullable [] getChildIndex() {
    if (childIndex == null) {
      return null;
    }
    return childIndex.clone();
  }

  /**
   * Get extraction byte positions for MultiMask layout.
   *
   * @return copy of extraction positions, or null if not MultiMask
   */
  public byte @Nullable [] getExtractionPositions() {
    return extractionPositions != null ? extractionPositions.clone() : null;
  }

  /**
   * Get extraction masks for MultiMask layout.
   *
   * @return copy of extraction masks, or null if not MultiMask
   */
  public long @Nullable [] getExtractionMasks() {
    return extractionMasks != null ? extractionMasks.clone() : null;
  }

  /**
   * Get number of extraction bytes for MultiMask layout.
   *
   * @return number of extraction bytes
   */
  public int getNumExtractionBytes() {
    return numExtractionBytes;
  }

  /**
   * Get total number of discriminative bits, regardless of layout type.
   *
   * @return total discriminative bits
   */
  public int getTotalDiscBits() {
    if (layoutType == LayoutType.MULTI_MASK && extractionMasks != null) {
      int total = 0;
      for (final long mask : extractionMasks) {
        total += Long.bitCount(mask);
      }
      return total;
    }
    return Long.bitCount(bitMask);
  }

  /**
   * Create a copy with a new page key.
   *
   * @param newPageKey the new page key
   * @param newRevision the new revision
   * @return the copy with new page key
   */
  public HOTIndirectPage copyWithNewPageKey(long newPageKey, int newRevision) {
    HOTIndirectPage copy = new HOTIndirectPage(newPageKey, newRevision, this.height, this.nodeType, this.numChildren);
    copy.layoutType = this.layoutType;
    copy.initialBytePos = this.initialBytePos;
    copy.bitMask = this.bitMask;
    copy.mostSignificantBitIndex = this.mostSignificantBitIndex;

    if (this.extractionPositions != null) {
      copy.extractionPositions = this.extractionPositions.clone();
    }
    if (this.extractionMasks != null) {
      copy.extractionMasks = this.extractionMasks.clone();
    }
    copy.numExtractionBytes = this.numExtractionBytes;
    if (this.bitPositions != null) {
      copy.bitPositions = this.bitPositions.clone();
    }
    if (this.partialKeys != null) {
      copy.partialKeys = this.partialKeys.clone();
      copy.sparsePartialKeys = createSparsePartialKeys(
          this.partialKeys, this.numChildren, this.getPartialKeyWidth());
    }
    if (this.childIndex != null) {
      copy.childIndex = this.childIndex.clone();
    }

    for (int i = 0; i < this.numChildren; i++) {
      if (this.childReferences[i] != null) {
        copy.childReferences[i] = new PageReference(this.childReferences[i]);
      }
    }

    return copy;
  }

  /**
   * Create a copy of this node with one child reference updated.
   * 
   * <p>
   * This is used when a child splits and the parent needs to point to the new subtree (BiNode)
   * containing the split children.
   * </p>
   * 
   * @param childIndex the index of the child to update
   * @param newChildRef the new child reference
   * @param newRevision the new revision number
   * @return a new HOTIndirectPage with the updated child
   */
  public HOTIndirectPage withUpdatedChild(int childIndex, PageReference newChildRef, int newRevision) {
    if (childIndex < 0 || childIndex >= numChildren) {
      throw new IllegalArgumentException("Invalid child index: " + childIndex + ", numChildren: " + numChildren);
    }

    // Create a copy with same page key but new revision
    HOTIndirectPage copy = copyWithNewPageKey(this.pageKey, newRevision);

    // Update the specified child reference
    copy.childReferences[childIndex] = newChildRef;

    return copy;
  }

  /**
   * Create a new SpanNode with explicit height.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param initialBytePos initial byte position for SINGLE_MASK
   * @param bitMask the 64-bit mask for extracting discriminative bits
   * @param partialKeys array of partial keys (extracted bits for each child)
   * @param children array of child references
   * @param height the height of this node
   * @return new SpanNode
   */
  public static HOTIndirectPage createSpanNode(long pageKey, int revision, int initialBytePos, long bitMask,
      int[] partialKeys, PageReference[] children, int height) {
    if (children.length < 2 || children.length > 16) {
      throw new IllegalArgumentException("SpanNode must have 2-16 children");
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, height, NodeType.SPAN_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = initialBytePos;
    page.bitMask = bitMask;
    page.mostSignificantBitIndex = computeMostSignificantBitIndex(initialBytePos, bitMask);
    page.partialKeys = partialKeys.clone();
    page.sparsePartialKeys = createSparsePartialKeys(partialKeys, children.length, determinePartialKeyWidth(bitMask));
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  /**
   * Create a new MultiNode with explicit height.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param initialBytePos initial byte position
   * @param bitMask the 64-bit mask for extracting discriminative bits
   * @param partialKeys array of partial keys
   * @param children array of child references
   * @param height the height of this node
   * @return new MultiNode
   */
  public static HOTIndirectPage createMultiNode(long pageKey, int revision, int initialBytePos, long bitMask,
      int[] partialKeys, PageReference[] children, int height) {
    if (children.length < 1 || children.length > 32) {
      throw new IllegalArgumentException("MultiNode must have 1-32 children, got: " + children.length);
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, height, NodeType.MULTI_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = initialBytePos;
    page.bitMask = bitMask;
    page.mostSignificantBitIndex = computeMostSignificantBitIndex(initialBytePos, bitMask);
    page.partialKeys = partialKeys.clone();
    page.sparsePartialKeys = createSparsePartialKeys(partialKeys, children.length, determinePartialKeyWidth(bitMask));
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  // ===== MultiMask factory methods (C++ reference: MultiMaskPartialKeyMapping) =====

  /**
   * Create a SpanNode with MultiMask layout (discriminative bits span >8 bytes).
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param extractionPositions key byte positions to gather (one per extraction byte)
   * @param extractionMasks PEXT masks per 8-byte chunk (packed LE)
   * @param numExtractionBytes number of extraction bytes used
   * @param partialKeys array of partial keys
   * @param children array of child references
   * @param height the height of this node
   * @param mostSignificantBitIndex the MSB index (smallest absolute disc bit position)
   * @return new SpanNode with MultiMask layout
   */
  public static HOTIndirectPage createSpanNodeMultiMask(long pageKey, int revision,
      byte[] extractionPositions, long[] extractionMasks, int numExtractionBytes,
      int[] partialKeys, PageReference[] children, int height, short mostSignificantBitIndex) {
    if (children.length < 2 || children.length > 16) {
      throw new IllegalArgumentException("SpanNode must have 2-16 children");
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, height, NodeType.SPAN_NODE, children.length);
    page.layoutType = LayoutType.MULTI_MASK;
    page.extractionPositions = extractionPositions.clone();
    page.extractionMasks = extractionMasks.clone();
    page.numExtractionBytes = numExtractionBytes;
    page.mostSignificantBitIndex = mostSignificantBitIndex;
    page.partialKeys = partialKeys.clone();
    int totalDiscBits = 0;
    for (final long mask : extractionMasks) {
      totalDiscBits += Long.bitCount(mask);
    }
    page.sparsePartialKeys = createSparsePartialKeys(partialKeys, children.length,
        determinePartialKeyWidthFromBitCount(totalDiscBits));
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  /**
   * Create a MultiNode with MultiMask layout (discriminative bits span >8 bytes).
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param extractionPositions key byte positions to gather
   * @param extractionMasks PEXT masks per 8-byte chunk (packed LE)
   * @param numExtractionBytes number of extraction bytes used
   * @param partialKeys array of partial keys
   * @param children array of child references
   * @param height the height of this node
   * @param mostSignificantBitIndex the MSB index
   * @return new MultiNode with MultiMask layout
   */
  public static HOTIndirectPage createMultiNodeMultiMask(long pageKey, int revision,
      byte[] extractionPositions, long[] extractionMasks, int numExtractionBytes,
      int[] partialKeys, PageReference[] children, int height, short mostSignificantBitIndex) {
    if (children.length < 1 || children.length > 32) {
      throw new IllegalArgumentException("MultiNode must have 1-32 children, got: " + children.length);
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, height, NodeType.MULTI_NODE, children.length);
    page.layoutType = LayoutType.MULTI_MASK;
    page.extractionPositions = extractionPositions.clone();
    page.extractionMasks = extractionMasks.clone();
    page.numExtractionBytes = numExtractionBytes;
    page.mostSignificantBitIndex = mostSignificantBitIndex;
    page.partialKeys = partialKeys.clone();
    int totalDiscBits = 0;
    for (final long mask : extractionMasks) {
      totalDiscBits += Long.bitCount(mask);
    }
    page.sparsePartialKeys = createSparsePartialKeys(partialKeys, children.length,
        determinePartialKeyWidthFromBitCount(totalDiscBits));
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  // ===== Page interface implementation =====

  @Override
  public List<PageReference> getReferences() {
    List<PageReference> refs = new ArrayList<>(numChildren);
    for (int i = 0; i < numChildren; i++) {
      if (childReferences[i] != null) {
        refs.add(childReferences[i]);
      }
    }
    return refs;
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    if (offset < 0 || offset >= numChildren) {
      return null;
    }
    if (childReferences[offset] == null) {
      childReferences[offset] = new PageReference();
    }
    return childReferences[offset];
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    if (offset < 0 || offset >= childReferences.length) {
      return true; // Page full
    }
    childReferences[offset] = pageReference;
    return false;
  }

  @Override
  public void commit(io.sirix.api.StorageEngineWriter storageEngineWriter) {
    // Commit all child pages before this page is written
    for (int i = 0; i < numChildren; i++) {
      PageReference ref = childReferences[i];
      if (ref != null && ref.getLogKey() != io.sirix.settings.Constants.NULL_ID_INT) {
        storageEngineWriter.commit(ref);
      }
    }
  }

  @Override
  public String toString() {
    return "HOTIndirectPage{" + "pageKey=" + pageKey + ", revision=" + revision + ", height=" + height + ", nodeType="
        + nodeType + ", layoutType=" + layoutType + ", numChildren=" + numChildren
        + ", msbIndex=" + mostSignificantBitIndex + '}';
  }
}

