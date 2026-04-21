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

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.Allocators;
import io.sirix.index.hot.DiscriminativeBitComputer;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.settings.DiagnosticSettings;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HOT (Height Optimized Trie) leaf page for cache-friendly secondary indexes.
 * 
 * <p>
 * Stores sorted key-value entries with off-heap MemorySegment storage. Implements KeyValuePage for
 * versioning compatibility with existing infrastructure.
 * </p>
 * 
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>Off-heap storage via MemorySegment (zero-copy deserialization)</li>
 * <li>Sorted entries for O(log n) binary search within page</li>
 * <li>Guard-based lifetime management (LeanStore/Umbra pattern)</li>
 * <li>No sibling pointers (COW-compatible)</li>
 * <li>SIMD-optimized key comparison via MemorySegment.mismatch()</li>
 * </ul>
 * 
 * <p>
 * <b>Memory Layout:</b>
 * </p>
 * 
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │ Entry format: [u16 keyLen][key bytes][u16 valueLen][value bytes]            │
 * │ Entries are stored contiguously in slotMemory, offsets in slotOffsets array │
 * └─────────────────────────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * @author Johannes Lichtenberger
 * @see KeyValuePage
 * @see HOTIndirectPage
 */
public final class HOTLeafPage implements KeyValuePage<DataRecord> {

  /** Sentinel value for "not found" in binary search. */
  public static final int NOT_FOUND = -1;

  /** Default page size for off-heap allocation (64KB). */
  public static final int DEFAULT_SIZE = 64 * 1024;

  /** Maximum entries per page before split. */
  public static final int MAX_ENTRIES = 512;

  /** Maximum entries for PEXT fast-path (SparsePartialKeys / SIMD equality limit). */
  private static final int PEXT_MAX_ENTRIES = 32;

  /** Maximum length for keys and values — must fit in an unsigned short (2 bytes). */
  private static final int MAX_KEY_VALUE_LENGTH = 0xFFFF;

  /** Empty prefix constant. */
  private static final byte[] EMPTY_PREFIX = new byte[0];

  /**
   * Unaligned short layout for zero-copy deserialization. When slotMemory is a slice, it may not be
   * 2-byte aligned.
   */
  private static final ValueLayout.OfShort JAVA_SHORT_UNALIGNED = ValueLayout.JAVA_SHORT.withByteAlignment(1);

  /**
   * Unaligned big-endian long layout for zero-allocation lexicographic key comparison.
   * Big-endian ensures that Long.compareUnsigned correctly orders byte sequences lexicographically.
   */
  private static final ValueLayout.OfLong JAVA_LONG_BE_UNALIGNED =
      ValueLayout.JAVA_LONG.withByteAlignment(1).withOrder(ByteOrder.BIG_ENDIAN);

  /**
   * Thread-local scratch buffer for compact() to avoid 2*n byte[] allocations.
   * Sized to DEFAULT_SIZE (64KB) to handle worst-case full page.
   */
  private static final ThreadLocal<byte[]> COMPACT_SCRATCH = ThreadLocal.withInitial(() -> new byte[DEFAULT_SIZE]);

  // ===== SIMD species for PEXT equality search =====
  private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;
  private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_256;
  private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_256;

  // ===== Page identity =====
  private final long recordPageKey;
  private final int revision;
  private final IndexType indexType;

  // ===== Off-heap storage =====
  // slotMemory stores entries as [u16 suffixLen][suffix bytes][u16 valueLen][value bytes]
  // where suffix = fullKey[commonPrefixLen..]. Full key = commonPrefix + suffix.
  private MemorySegment slotMemory;
  private Runnable releaser;
  private final int[] slotOffsets;
  private int entryCount;
  private int usedSlotMemorySize;

  // ===== Prefix compression =====
  // All keys in this leaf share commonPrefix. Entries store only the suffix.
  // Prefix is established on first insert and may shrink (rare) when a key with
  // shorter LCP arrives before the trie has a discriminative bit at that position.
  private byte[] commonPrefix;
  private int commonPrefixLen;

  // ===== PEXT routing within leaf (lazy-built, for ≤PEXT_MAX_ENTRIES only) =====
  // Discriminative bits over suffixes using single-mask layout (LE word, matches HOTIndirectPage).
  // Dense partial key = Long.compress(loadSuffixWordLE(suffix, discBytePos), discBitMask).
  // SIMD equality search finds entries with matching partial key; verify with full suffix comparison.
  private int discBytePos;            // Starting byte position in suffix for disc bits
  private long discBitMask;           // 64-bit PEXT mask over suffix bytes (LE word layout)
  private int discBitCount;           // Number of discriminative bits (= Long.bitCount(discBitMask))
  private byte[] densePKBytes;        // Dense partial keys per entry (for ≤8 disc bits)
  private short[] densePKShorts;      // Dense partial keys per entry (for 9-16 disc bits)
  private int[] densePKInts;          // Dense partial keys per entry (for 17-32 disc bits)
  private boolean pextValid;          // True if PEXT index is current

  // ===== Guard-based lifetime management (LeanStore/Umbra pattern) =====
  // Note: For production, consider using @Contended annotation to avoid false sharing
  // Padding fields prevent false sharing on guardCount AtomicInteger
  @SuppressWarnings("unused")
  private long p1, p2, p3, p4, p5, p6, p7; // Cache line padding (56 bytes before)
  private final AtomicInteger guardCount = new AtomicInteger(0);
  @SuppressWarnings("unused")
  private long p8, p9, p10, p11, p12, p13, p14; // Cache line padding (56 bytes after)
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile boolean isOrphaned = false;

  // ===== Version for detecting page reuse =====
  private final AtomicInteger version = new AtomicInteger(0);
  private volatile boolean hot = false;

  // ===== Page references for overflow entries =====
  private final it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<PageReference> pageReferences;

  // ===== Diagnostic tracking =====
  @SuppressWarnings("unused")
  private static final boolean DEBUG_MEMORY_LEAKS = DiagnosticSettings.MEMORY_LEAK_TRACKING;

  /**
   * Create a new HOTLeafPage with allocated off-heap memory.
   *
   * @param recordPageKey the page key
   * @param revision the revision number
   * @param indexType the index type (PATH, CAS, NAME)
   */
  public HOTLeafPage(long recordPageKey, int revision, IndexType indexType) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = Objects.requireNonNull(indexType);

    // Allocate off-heap memory
    MemorySegmentAllocator allocator = Allocators.getInstance();
    this.slotMemory = allocator.allocate(DEFAULT_SIZE);
    // Capture by local variable to avoid releasing wrong memory if slotMemory field is later changed
    final MemorySegment segmentToRelease = this.slotMemory;
    this.releaser = () -> allocator.release(segmentToRelease);

    this.slotOffsets = new int[MAX_ENTRIES];
    this.entryCount = 0;
    this.usedSlotMemorySize = 0;
    this.commonPrefix = EMPTY_PREFIX;
    this.commonPrefixLen = 0;
    this.pageReferences = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>();
  }

  /**
   * Create a HOTLeafPage with provided memory segment (for deserialization).
   *
   * @param recordPageKey the page key
   * @param revision the revision number
   * @param indexType the index type
   * @param slotMemory the off-heap memory segment containing suffix-based entries
   * @param releaser the releaser for the memory segment
   * @param slotOffsets the slot offsets array
   * @param entryCount the number of entries
   * @param usedSlotMemorySize the used slot memory size
   * @param commonPrefix the common prefix shared by all keys
   * @param commonPrefixLen the length of the common prefix
   */
  public HOTLeafPage(long recordPageKey, int revision, IndexType indexType, MemorySegment slotMemory,
      @Nullable Runnable releaser, int[] slotOffsets, int entryCount, int usedSlotMemorySize,
      byte[] commonPrefix, int commonPrefixLen) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = Objects.requireNonNull(indexType);
    this.slotMemory = Objects.requireNonNull(slotMemory);
    this.releaser = releaser;
    this.slotOffsets = slotOffsets;
    this.entryCount = entryCount;
    this.usedSlotMemorySize = usedSlotMemorySize;
    this.commonPrefix = commonPrefix != null ? commonPrefix : EMPTY_PREFIX;
    this.commonPrefixLen = commonPrefixLen;
    this.pageReferences = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>();
    // Eagerly build PEXT index so read-only lookups after deserialization
    // use PEXT-accelerated search immediately (no first-search latency spike).
    buildPextIndex();
  }

  /**
   * Legacy deserialization constructor (no prefix — for backward compatibility with V1 format).
   */
  public HOTLeafPage(long recordPageKey, int revision, IndexType indexType, MemorySegment slotMemory,
      @Nullable Runnable releaser, int[] slotOffsets, int entryCount, int usedSlotMemorySize) {
    this(recordPageKey, revision, indexType, slotMemory, releaser, slotOffsets, entryCount, usedSlotMemorySize,
        EMPTY_PREFIX, 0);
  }

  // ===== Suffix access (zero-copy from slotMemory) =====

  /**
   * Get suffix slice from off-heap segment (zero-copy). Suffixes are stored as the entry key
   * in slotMemory: {@code [u16 suffixLen][suffix bytes][u16 valueLen][value bytes]}.
   *
   * @param index the entry index
   * @return the suffix as a MemorySegment slice
   */
  private MemorySegment getSuffixSlice(int index) {
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    return slotMemory.asSlice(offset + 2, suffixLen);
  }

  /**
   * Get suffix as byte array (copies data).
   *
   * @param index the entry index
   * @return the suffix as byte array
   */
  private byte[] getSuffix(int index) {
    final MemorySegment slice = getSuffixSlice(index);
    final byte[] suffix = new byte[(int) slice.byteSize()];
    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, suffix, 0, suffix.length);
    return suffix;
  }

  // ===== PEXT index building =====

  /**
   * Build the PEXT routing index over entry suffixes. Only built when
   * {@code 2 <= entryCount <= PEXT_MAX_ENTRIES}. Uses single-mask layout
   * (LE word at {@link #discBytePos} + 64-bit {@link #discBitMask}).
   *
   * <p>For each adjacent pair of sorted suffixes, computes the first differing
   * bit position. All these bit positions are combined into a single PEXT mask.
   * Then for each entry, the dense partial key is extracted and stored in a
   * type-appropriate array for SIMD equality search.</p>
   */
  private void buildPextIndex() {
    if (entryCount < 2 || entryCount > PEXT_MAX_ENTRIES) {
      pextValid = false;
      densePKBytes = null;
      densePKShorts = null;
      densePKInts = null;
      return;
    }

    // Collect all discriminative bit positions among adjacent suffix pairs.
    // Bit positions are in MSB-first convention (byte 0, bit 0 = MSB of first suffix byte).
    // We need to convert to LE word layout for PEXT consistency with HOTIndirectPage.
    int minBytePos = Integer.MAX_VALUE;
    int maxBytePos = 0;
    final int[] diffBits = new int[entryCount - 1];
    boolean hasDiffBits = false;

    for (int i = 0; i < entryCount - 1; i++) {
      final MemorySegment s1 = getSuffixSlice(i);
      final MemorySegment s2 = getSuffixSlice(i + 1);
      final int diffBit = DiscriminativeBitComputer.computeDifferingBit(s1, s2);
      diffBits[i] = diffBit;
      if (diffBit >= 0) {
        hasDiffBits = true;
        final int bytePos = diffBit / 8;
        minBytePos = Math.min(minBytePos, bytePos);
        maxBytePos = Math.max(maxBytePos, bytePos);
      }
    }

    if (!hasDiffBits) {
      pextValid = false;
      return;
    }

    // Check if disc bits span ≤ 8 bytes (fits in single long for PEXT)
    if (maxBytePos - minBytePos >= 8) {
      // Disc bits too spread out — fall back to binary search
      pextValid = false;
      return;
    }

    discBytePos = minBytePos;

    // Build LE word bit mask: for each disc bit position, compute its position
    // within the 8-byte LE word starting at discBytePos
    long mask = 0;
    for (int i = 0; i < entryCount - 1; i++) {
      if (diffBits[i] >= 0) {
        final int absBitPos = diffBits[i]; // byte*8 + bitInByte (MSB-first)
        final int bytePos = absBitPos / 8;
        final int bitInByte = absBitPos % 8; // 0=MSB, 7=LSB
        // LE word layout: byte at bytePos maps to bits [(bytePos-discBytePos)*8 .. +7]
        // Within byte: MSB(bit 0) → position 7, LSB(bit 7) → position 0
        final int bitInWord = (bytePos - discBytePos) * 8 + (7 - bitInByte);
        mask |= 1L << bitInWord;
      }
    }

    discBitMask = mask;
    discBitCount = Long.bitCount(mask);

    if (discBitCount > 32) {
      // Too many disc bits for int partial key — fall back
      pextValid = false;
      return;
    }

    // Compute dense partial key for each entry
    if (discBitCount <= 8) {
      densePKBytes = new byte[PEXT_MAX_ENTRIES]; // Pad to 32 for SIMD alignment
      densePKShorts = null;
      densePKInts = null;
      for (int i = 0; i < entryCount; i++) {
        densePKBytes[i] = (byte) computeSuffixPartialKey(i);
      }
    } else if (discBitCount <= 16) {
      densePKBytes = null;
      densePKShorts = new short[PEXT_MAX_ENTRIES];
      densePKInts = null;
      for (int i = 0; i < entryCount; i++) {
        densePKShorts[i] = (short) computeSuffixPartialKey(i);
      }
    } else {
      densePKBytes = null;
      densePKShorts = null;
      densePKInts = new int[PEXT_MAX_ENTRIES];
      for (int i = 0; i < entryCount; i++) {
        densePKInts[i] = computeSuffixPartialKey(i);
      }
    }

    pextValid = true;
  }

  /**
   * Compute the PEXT partial key for the suffix at the given entry index.
   */
  private int computeSuffixPartialKey(int index) {
    final MemorySegment suffix = getSuffixSlice(index);
    return computePartialKeyFromSegment(suffix, discBytePos, discBitMask);
  }

  /**
   * Compute PEXT partial key from a MemorySegment suffix.
   */
  private static int computePartialKeyFromSegment(MemorySegment suffix, int bytePos, long bitMask) {
    final long suffixWord = loadWordLE(suffix, bytePos);
    return (int) Long.compress(suffixWord, bitMask);
  }

  /**
   * Compute PEXT partial key from a byte array suffix (starting at given offset).
   */
  private static int computePartialKeyFromArray(byte[] key, int suffixOffset, int bytePos, long bitMask) {
    final long suffixWord = loadWordLEFromArray(key, suffixOffset + bytePos);
    return (int) Long.compress(suffixWord, bitMask);
  }

  /**
   * Load up to 8 bytes from a MemorySegment in LE word layout (byte at pos → bits 0-7).
   * Matches HOTIndirectPage.getKeyWordAt() byte ordering for PEXT consistency.
   */
  private static long loadWordLE(MemorySegment segment, int pos) {
    long result = 0;
    final long segLen = segment.byteSize();
    final int end = (int) Math.min(pos + 8, segLen);
    for (int i = pos; i < end; i++) {
      result |= ((long) (segment.get(ValueLayout.JAVA_BYTE, i) & 0xFF)) << ((i - pos) * 8);
    }
    return result;
  }

  /**
   * Load up to 8 bytes from a byte array in LE word layout.
   */
  private static long loadWordLEFromArray(byte[] key, int pos) {
    long result = 0;
    final int end = Math.min(pos + 8, key.length);
    for (int i = pos; i < end; i++) {
      result |= ((long) (key[i] & 0xFF)) << ((i - pos) * 8);
    }
    return result;
  }

  // ===== PEXT-routed search with binary search fallback =====

  /**
   * Find entry index for key using PEXT-accelerated search with prefix compression.
   *
   * <p><b>Algorithm:</b></p>
   * <ol>
   *   <li>Check that key starts with commonPrefix (short-circuit if not)</li>
   *   <li>For ≤32 entries with valid PEXT: SIMD equality search on partial keys → verify suffix</li>
   *   <li>Otherwise: binary search comparing suffixes (shorter keys = faster comparison)</li>
   * </ol>
   *
   * @param key the search key (full key including prefix)
   * @return index if found, or -(insertionPoint + 1) if not found
   */
  public int findEntry(byte[] key) {
    Objects.requireNonNull(key);

    if (entryCount == 0) {
      return -1;
    }

    // --- Prefix check ---
    if (commonPrefixLen > 0) {
      if (key.length < commonPrefixLen) {
        // Key shorter than prefix → compare to determine insertion point
        return computeInsertionPointFromPrefix(key);
      }
      // Compare key prefix with commonPrefix
      for (int i = 0; i < commonPrefixLen; i++) {
        if (key[i] != commonPrefix[i]) {
          // Key doesn't match prefix. Determine if key < prefix or key > prefix
          final int cmp = (key[i] & 0xFF) - (commonPrefix[i] & 0xFF);
          return cmp < 0 ? -1 : -(entryCount + 1);
        }
      }
    }

    // --- PEXT fast path (for ≤PEXT_MAX_ENTRIES entries) ---
    if (!pextValid && entryCount >= 2 && entryCount <= PEXT_MAX_ENTRIES) {
      buildPextIndex();
    }

    if (pextValid) {
      return pextSearch(key);
    }

    // --- Binary search on suffixes ---
    return binarySearchSuffix(key);
  }

  /**
   * Compute the insertion point when key doesn't match the common prefix.
   * If key &lt; prefix, insertion point is before all entries (return -1).
   * If key &gt; prefix, insertion point is after all entries (return -(entryCount+1)).
   */
  private int computeInsertionPointFromPrefix(byte[] key) {
    final int minLen = Math.min(key.length, commonPrefixLen);
    for (int i = 0; i < minLen; i++) {
      final int cmp = (key[i] & 0xFF) - (commonPrefix[i] & 0xFF);
      if (cmp < 0) return -1;
      if (cmp > 0) return -(entryCount + 1);
    }
    // key is a prefix of commonPrefix → key < all entries (prefix is shorter)
    return -1;
  }

  /**
   * PEXT-accelerated search: compute partial key from suffix, SIMD equality search,
   * verify candidates with full suffix comparison.
   */
  private int pextSearch(byte[] key) {
    final int searchPK = computePartialKeyFromArray(key, commonPrefixLen, discBytePos, discBitMask);

    // SIMD equality search on dense partial keys
    int candidates;
    if (discBitCount <= 8) {
      candidates = simdEqualitySearchBytes((byte) searchPK);
    } else if (discBitCount <= 16) {
      candidates = simdEqualitySearchShorts((short) searchPK);
    } else {
      candidates = simdEqualitySearchInts(searchPK);
    }

    // Verify candidates with full suffix comparison
    while (candidates != 0) {
      final int idx = Integer.numberOfTrailingZeros(candidates);
      if (suffixMatchesKey(idx, key)) {
        return idx;
      }
      candidates &= candidates - 1; // clear lowest bit
    }

    // Not found — compute insertion point via binary search
    return binarySearchSuffix(key);
  }

  /**
   * SIMD equality search on 8-bit dense partial keys. All 32 entries compared in one AVX2 op.
   */
  private int simdEqualitySearchBytes(byte searchPK) {
    if (BYTE_SPECIES.length() >= PEXT_MAX_ENTRIES) {
      final ByteVector searchVec = ByteVector.broadcast(BYTE_SPECIES, searchPK);
      final ByteVector entriesVec = ByteVector.fromArray(BYTE_SPECIES, densePKBytes, 0);
      final VectorMask<Byte> matches = searchVec.compare(VectorOperators.EQ, entriesVec);
      final long mask = entryCount == 32 ? 0xFFFFFFFFL : ((1L << entryCount) - 1);
      return (int) (matches.toLong() & mask);
    }
    // Scalar fallback
    int result = 0;
    for (int i = 0; i < entryCount; i++) {
      if (densePKBytes[i] == searchPK) {
        result |= (1 << i);
      }
    }
    return result;
  }

  /**
   * SIMD equality search on 16-bit dense partial keys.
   */
  private int simdEqualitySearchShorts(short searchPK) {
    if (SHORT_SPECIES.length() >= 16) {
      final ShortVector searchVec = ShortVector.broadcast(SHORT_SPECIES, searchPK);
      final ShortVector entriesVec = ShortVector.fromArray(SHORT_SPECIES, densePKShorts, 0);
      final VectorMask<Short> matches = searchVec.compare(VectorOperators.EQ, entriesVec);
      final long mask = entryCount == 16 ? 0xFFFFL : ((1L << Math.min(entryCount, 16)) - 1);
      int result = (int) (matches.toLong() & mask);
      if (entryCount > 16) {
        final ShortVector entriesVec2 = ShortVector.fromArray(SHORT_SPECIES, densePKShorts, 16);
        final VectorMask<Short> matches2 = searchVec.compare(VectorOperators.EQ, entriesVec2);
        final long mask2 = ((1L << (entryCount - 16)) - 1);
        result |= ((int) (matches2.toLong() & mask2)) << 16;
      }
      return result;
    }
    int result = 0;
    for (int i = 0; i < entryCount; i++) {
      if (densePKShorts[i] == searchPK) result |= (1 << i);
    }
    return result;
  }

  /**
   * SIMD equality search on 32-bit dense partial keys.
   */
  private int simdEqualitySearchInts(int searchPK) {
    if (INT_SPECIES.length() >= 8) {
      final IntVector searchVec = IntVector.broadcast(INT_SPECIES, searchPK);
      int result = 0;
      for (int chunk = 0; chunk < 4 && chunk * 8 < entryCount; chunk++) {
        final IntVector entriesVec = IntVector.fromArray(INT_SPECIES, densePKInts, chunk * 8);
        final VectorMask<Integer> matches = searchVec.compare(VectorOperators.EQ, entriesVec);
        result |= ((int) matches.toLong()) << (chunk * 8);
      }
      final long mask = entryCount == 32 ? 0xFFFFFFFFL : ((1L << entryCount) - 1);
      return (int) (result & mask);
    }
    int result = 0;
    for (int i = 0; i < entryCount; i++) {
      if (densePKInts[i] == searchPK) result |= (1 << i);
    }
    return result;
  }

  /**
   * Check if the suffix at the given entry index matches the search key's suffix portion.
   * Zero-allocation: compares MemorySegment suffix directly with key bytes starting at
   * commonPrefixLen.
   */
  private boolean suffixMatchesKey(int index, byte[] key) {
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int keySuffixLen = key.length - commonPrefixLen;

    if (suffixLen != keySuffixLen) {
      return false;
    }

    // Compare suffix bytes directly with key[commonPrefixLen..]
    final long suffixStart = offset + 2;
    for (int i = 0; i < suffixLen; i++) {
      if (slotMemory.get(ValueLayout.JAVA_BYTE, suffixStart + i) != key[commonPrefixLen + i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Binary search on suffixes. Compares the search key's suffix portion with stored suffixes.
   * Uses branchless comparison for better branch prediction.
   *
   * @param key the full search key
   * @return index if found, or -(insertionPoint + 1) if not found
   */
  private int binarySearchSuffix(byte[] key) {
    int low = 0;
    int high = entryCount;

    while (low < high) {
      final int mid = (low + high) >>> 1;
      final int cmp = compareSuffixWithKey(mid, key);
      low = cmp < 0 ? mid + 1 : low;
      high = cmp > 0 ? mid : high;
      if (cmp == 0) {
        return mid;
      }
    }
    return -(low + 1);
  }

  /**
   * Compare the suffix at entry index with the suffix portion of the search key.
   * Zero-allocation: reads directly from slotMemory and key array.
   *
   * @param index the entry index
   * @param key the full search key
   * @return negative if stored suffix &lt; key suffix, positive if &gt;, zero if equal
   */
  private int compareSuffixWithKey(int index, byte[] key) {
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final long suffixStart = offset + 2;
    final int keySuffixStart = commonPrefixLen;
    final int keySuffixLen = key.length - keySuffixStart;
    final int minLen = Math.min(suffixLen, keySuffixLen);

    // Fast path: compare 8 bytes at a time
    int i = 0;
    for (; i + 8 <= minLen; i += 8) {
      final long aWord = slotMemory.get(JAVA_LONG_BE_UNALIGNED, suffixStart + i);
      final long bWord = ((long) (key[keySuffixStart + i]     & 0xFF) << 56)
                       | ((long) (key[keySuffixStart + i + 1] & 0xFF) << 48)
                       | ((long) (key[keySuffixStart + i + 2] & 0xFF) << 40)
                       | ((long) (key[keySuffixStart + i + 3] & 0xFF) << 32)
                       | ((long) (key[keySuffixStart + i + 4] & 0xFF) << 24)
                       | ((long) (key[keySuffixStart + i + 5] & 0xFF) << 16)
                       | ((long) (key[keySuffixStart + i + 6] & 0xFF) << 8)
                       |  (long) (key[keySuffixStart + i + 7] & 0xFF);
      if (aWord != bWord) {
        return Long.compareUnsigned(aWord, bWord);
      }
    }

    // Compare remaining bytes
    for (; i < minLen; i++) {
      final int aByte = Byte.toUnsignedInt(slotMemory.get(ValueLayout.JAVA_BYTE, suffixStart + i));
      final int bByte = key[keySuffixStart + i] & 0xFF;
      if (aByte != bByte) {
        return Integer.compare(aByte, bByte);
      }
    }

    return Integer.compare(suffixLen, keySuffixLen);
  }

  // ===== Key/Value access (prefix + suffix reconstruction) =====

  /**
   * Get key slice as MemorySegment. Reconstructs the full key by concatenating
   * commonPrefix + suffix. Allocates a byte array (not zero-copy) because
   * prefix and suffix are not contiguous in memory.
   *
   * <p>For zero-copy suffix access (internal use), use {@link #getSuffixSlice(int)}.</p>
   *
   * @param index the entry index
   * @return the full key as a MemorySegment (backed by on-heap byte[])
   */
  public MemorySegment getKeySlice(int index) {
    Objects.checkIndex(index, entryCount);
    return MemorySegment.ofArray(getKey(index));
  }

  /**
   * Get value slice from off-heap segment (zero-copy).
   *
   * @param index the entry index
   * @return the value as a MemorySegment slice
   */
  public MemorySegment getValueSlice(int index) {
    Objects.checkIndex(index, entryCount);
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int valueOffset = offset + 2 + suffixLen;
    final int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));
    return slotMemory.asSlice(valueOffset + 2, valueLen);
  }

  /**
   * Get full key as byte array. Reconstructs commonPrefix + suffix.
   *
   * @param index the entry index
   * @return the full key as byte array
   */
  public byte[] getKey(int index) {
    Objects.checkIndex(index, entryCount);
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final byte[] fullKey = new byte[commonPrefixLen + suffixLen];
    if (commonPrefixLen > 0) {
      System.arraycopy(commonPrefix, 0, fullKey, 0, commonPrefixLen);
    }
    if (suffixLen > 0) {
      MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, offset + 2, fullKey, commonPrefixLen, suffixLen);
    }
    return fullKey;
  }

  /**
   * Get value as byte array (copies data).
   *
   * @param index the entry index
   * @return the value as byte array
   */
  public byte[] getValue(int index) {
    final MemorySegment slice = getValueSlice(index);
    final byte[] value = new byte[(int) slice.byteSize()];
    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, value, 0, value.length);
    return value;
  }

  // ===== Insert/Update operations =====

  /**
   * Insert or update an entry. Handles prefix establishment and shrinkage.
   *
   * @param key the full key
   * @param value the value
   * @return true if inserted, false if updated
   */
  public boolean put(byte[] key, byte[] value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);

    // Handle prefix for the incoming key
    handlePrefixForInsert(key);

    int index = findEntry(key);
    if (index >= 0) {
      // Update existing entry - for now, just mark as updated
      // A more sophisticated implementation would handle in-place updates
      return false;
    }

    // Insert new entry (suffix will be extracted from key)
    int insertPos = -(index + 1);
    return insertAtWithKey(insertPos, key, value);
  }

  /**
   * Zero-allocation variant of {@link #put(byte[], byte[])}: writes the
   * value range {@code [valueOff, valueOff+valueLen)} from {@code valueBuf}
   * directly into the slot heap without requiring the caller to slice its
   * payload into a standalone {@code byte[]} first.
   *
   * <p>HFT write path: the projection builder stores 20 KB leaves as 5×4 KB
   * chunks — the per-leaf arraycopy on this path alone was ~100 MB/s churn
   * at 100M scale. Letting {@link HOTLeafPage} consume the range directly
   * eliminates the intermediate allocation, dropping the per-chunk cost to
   * a single {@code MemorySegment.copy} against the pre-allocated heap.
   *
   * @param key the full key (routing bytes — the trie caller already
   *            navigated here, so this is used only for suffix extraction
   *            and binary-search ordering).
   * @param valueBuf buffer holding the value range
   * @param valueOff offset in {@code valueBuf} where the value starts
   * @param valueLen length of the value range
   * @return {@code true} on successful insert; {@code false} if the key
   *         already exists (caller should fall through to {@link #updateValue})
   *         or the page is full (caller should split).
   */
  public boolean putRange(byte[] key, byte[] valueBuf, int valueOff, int valueLen) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(valueBuf);
    if (valueOff < 0 || valueLen < 0 || valueOff + valueLen > valueBuf.length) {
      throw new IndexOutOfBoundsException(
          "valueOff=" + valueOff + " valueLen=" + valueLen + " valueBuf.length=" + valueBuf.length);
    }

    handlePrefixForInsert(key);

    int index = findEntry(key);
    if (index >= 0) {
      return false; // existing — caller handles via updateValue
    }

    int insertPos = -(index + 1);
    final int suffixLen = key.length - commonPrefixLen;
    return insertAtSuffixRange(insertPos, key, commonPrefixLen, suffixLen, valueBuf, valueOff, valueLen);
  }

  /**
   * Insert entry at {@code pos} using a value slice from {@code valueBuf}.
   * Identical to {@link #insertAtSuffix} apart from consuming a range
   * rather than a stand-alone array.
   */
  private boolean insertAtSuffixRange(int pos, byte[] keyBuf, int suffixOffset, int suffixLen,
      byte[] valueBuf, int valueOff, int valueLen) {
    if (suffixLen > MAX_KEY_VALUE_LENGTH) {
      throw new IllegalArgumentException("Suffix length " + suffixLen + " exceeds maximum " + MAX_KEY_VALUE_LENGTH);
    }
    if (valueLen > MAX_KEY_VALUE_LENGTH) {
      throw new IllegalArgumentException("Value length " + valueLen + " exceeds maximum " + MAX_KEY_VALUE_LENGTH);
    }
    if (entryCount >= MAX_ENTRIES) {
      return false;
    }

    ensureMutableSlotMemory();

    final int entrySize = 2 + suffixLen + 2 + valueLen;

    if (usedSlotMemorySize + entrySize > slotMemory.byteSize()) {
      compact();
      if (usedSlotMemorySize + entrySize > slotMemory.byteSize()) {
        return false;
      }
    }

    if (pos < entryCount) {
      System.arraycopy(slotOffsets, pos, slotOffsets, pos + 1, entryCount - pos);
    }

    int offset = usedSlotMemorySize;
    slotOffsets[pos] = offset;

    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) suffixLen);
    offset += 2;
    if (suffixLen > 0) {
      MemorySegment.copy(keyBuf, suffixOffset, slotMemory, ValueLayout.JAVA_BYTE, offset, suffixLen);
    }
    offset += suffixLen;
    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) valueLen);
    offset += 2;
    if (valueLen > 0) {
      MemorySegment.copy(valueBuf, valueOff, slotMemory, ValueLayout.JAVA_BYTE, offset, valueLen);
    }

    usedSlotMemorySize += entrySize;
    entryCount++;
    pextValid = false;

    return true;
  }

  /**
   * MemorySegment-native variant of {@link #putRange(byte[], byte[], int, int)}:
   * consumes the value directly from an off-heap {@code MemorySegment}
   * — no intermediate byte[]. Matches the pattern
   * {@link io.sirix.page.KeyValueLeafPage} uses for writing slot payloads
   * straight from off-heap buffers.
   *
   * <p>HFT use case: the projection builder serialises into a reusable
   * off-heap scratch segment, then slices 4 KB ranges from that segment
   * directly into the HOT leaf's {@code slotMemory} via
   * {@link MemorySegment#copy(MemorySegment, long, MemorySegment, long, long)}.
   * Zero heap allocation per chunk, zero GC pressure at commit time.
   */
  public boolean putRange(byte[] key, MemorySegment valueSrc, long valueOff, int valueLen) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(valueSrc);
    if (valueOff < 0 || valueLen < 0 || valueOff + valueLen > valueSrc.byteSize()) {
      throw new IndexOutOfBoundsException(
          "valueOff=" + valueOff + " valueLen=" + valueLen + " segBytes=" + valueSrc.byteSize());
    }

    handlePrefixForInsert(key);

    int index = findEntry(key);
    if (index >= 0) {
      return false;
    }

    int insertPos = -(index + 1);
    final int suffixLen = key.length - commonPrefixLen;
    return insertAtSuffixSegmentRange(insertPos, key, commonPrefixLen, suffixLen, valueSrc, valueOff, valueLen);
  }

  private boolean insertAtSuffixSegmentRange(int pos, byte[] keyBuf, int suffixOffset, int suffixLen,
      MemorySegment valueSrc, long valueOff, int valueLen) {
    if (suffixLen > MAX_KEY_VALUE_LENGTH) {
      throw new IllegalArgumentException("Suffix length " + suffixLen + " exceeds maximum " + MAX_KEY_VALUE_LENGTH);
    }
    if (valueLen > MAX_KEY_VALUE_LENGTH) {
      throw new IllegalArgumentException("Value length " + valueLen + " exceeds maximum " + MAX_KEY_VALUE_LENGTH);
    }
    if (entryCount >= MAX_ENTRIES) {
      return false;
    }

    ensureMutableSlotMemory();

    final int entrySize = 2 + suffixLen + 2 + valueLen;

    if (usedSlotMemorySize + entrySize > slotMemory.byteSize()) {
      compact();
      if (usedSlotMemorySize + entrySize > slotMemory.byteSize()) {
        return false;
      }
    }

    if (pos < entryCount) {
      System.arraycopy(slotOffsets, pos, slotOffsets, pos + 1, entryCount - pos);
    }

    int offset = usedSlotMemorySize;
    slotOffsets[pos] = offset;

    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) suffixLen);
    offset += 2;
    if (suffixLen > 0) {
      MemorySegment.copy(keyBuf, suffixOffset, slotMemory, ValueLayout.JAVA_BYTE, offset, suffixLen);
    }
    offset += suffixLen;
    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) valueLen);
    offset += 2;
    if (valueLen > 0) {
      // Pure off-heap → off-heap copy. On Linux this is a single
      // memcpy call — no per-element bounds check, no heap touch.
      MemorySegment.copy(valueSrc, valueOff, slotMemory, offset, valueLen);
    }

    usedSlotMemorySize += entrySize;
    entryCount++;
    pextValid = false;

    return true;
  }

  /**
   * MemorySegment-native variant of {@link #updateValueRange(int, byte[], int, int)}:
   * in-place update from an off-heap segment range. Returns {@code true}
   * iff the new value length matches the existing slot (in-place). Size
   * change falls back to the caller's copying path.
   */
  public boolean updateValueRange(int index, MemorySegment valueSrc, long valueOff, int valueLen) {
    if (index < 0 || index >= entryCount) {
      throw new IndexOutOfBoundsException("index=" + index + " entryCount=" + entryCount);
    }
    if (valueOff < 0 || valueLen < 0 || valueOff + valueLen > valueSrc.byteSize()) {
      throw new IndexOutOfBoundsException(
          "valueOff=" + valueOff + " valueLen=" + valueLen + " segBytes=" + valueSrc.byteSize());
    }
    ensureMutableSlotMemory();
    final int entryOffset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, entryOffset));
    final int valueLenOffset = entryOffset + 2 + suffixLen;
    final int oldValueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueLenOffset));
    if (valueLen == oldValueLen) {
      if (valueLen > 0) {
        MemorySegment.copy(valueSrc, valueOff, slotMemory, valueLenOffset + 2, valueLen);
      }
      pextValid = false;
      return true;
    }
    return false;
  }

  /**
   * Zero-allocation variant of {@link #updateValue(int, byte[])}: writes
   * {@code [valueOff, valueOff+valueLen)} from {@code valueBuf} as the new
   * value for the entry at {@code index}. Falls back to the copying path
   * if the new value doesn't fit the existing slot, returning {@code false}.
   */
  public boolean updateValueRange(int index, byte[] valueBuf, int valueOff, int valueLen) {
    if (index < 0 || index >= entryCount) {
      throw new IndexOutOfBoundsException("index=" + index + " entryCount=" + entryCount);
    }
    if (valueOff < 0 || valueLen < 0 || valueOff + valueLen > valueBuf.length) {
      throw new IndexOutOfBoundsException(
          "valueOff=" + valueOff + " valueLen=" + valueLen + " valueBuf.length=" + valueBuf.length);
    }
    ensureMutableSlotMemory();
    final int entryOffset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, entryOffset));
    final int valueLenOffset = entryOffset + 2 + suffixLen;
    final int oldValueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueLenOffset));

    if (valueLen == oldValueLen) {
      // In-place update — the common case for projection chunk rewrites
      // where the chunk size stays the same across revisions. Zero growth,
      // zero compaction — just overwrite the existing byte range.
      if (valueLen > 0) {
        MemorySegment.copy(valueBuf, valueOff, slotMemory, ValueLayout.JAVA_BYTE, valueLenOffset + 2, valueLen);
      }
      pextValid = false;
      return true;
    }
    // Size changed — fall back to the original path (re-insert with
    // compaction). Signalled by returning false so the caller can choose
    // to build a copied byte[] and call updateValue.
    return false;
  }

  /**
   * Establish or update the common prefix when inserting a new key.
   *
   * <p>On first insert: commonPrefix = key (suffix = empty).
   * On subsequent inserts: if LCP(key, commonPrefix) &lt; commonPrefixLen,
   * shrink prefix and extend all existing suffixes.</p>
   */
  private void handlePrefixForInsert(byte[] key) {
    if (entryCount == 0) {
      // First entry: set prefix to entire key
      commonPrefix = key.clone();
      commonPrefixLen = key.length;
      return;
    }

    // Compute LCP of key with current prefix
    final int lcp = longestCommonPrefix(commonPrefix, commonPrefixLen, key, key.length);

    if (lcp < commonPrefixLen) {
      // Prefix must shrink — extend all existing suffixes
      rebuildForShorterPrefix(lcp);
    }
  }

  /**
   * Compute the longest common prefix length between two byte sequences.
   */
  private static int longestCommonPrefix(byte[] a, int aLen, byte[] b, int bLen) {
    final int minLen = Math.min(aLen, bLen);
    for (int i = 0; i < minLen; i++) {
      if (a[i] != b[i]) {
        return i;
      }
    }
    return minLen;
  }

  /**
   * Rebuild the page with a shorter common prefix. All existing suffixes are extended
   * by prepending the bytes that were removed from the prefix.
   *
   * <p>This is expensive (rewrites all entries) but rare — it only happens when a key
   * arrives that shares fewer bytes with the common prefix than all existing entries.
   * After a trie split, this becomes impossible because the discriminative bit will
   * separate such keys.</p>
   *
   * @param newPrefixLen the new (shorter) prefix length
   */
  private void rebuildForShorterPrefix(int newPrefixLen) {
    if (newPrefixLen >= commonPrefixLen) {
      return; // No change needed
    }

    ensureMutableSlotMemory();

    // The bytes being removed from the prefix that must be prepended to each suffix
    final int extensionLen = commonPrefixLen - newPrefixLen;
    final byte[] extension = new byte[extensionLen];
    System.arraycopy(commonPrefix, newPrefixLen, extension, 0, extensionLen);

    // Rebuild all entries with extended suffixes
    // Use scratch buffer to avoid quadratic allocation
    final byte[] scratch = COMPACT_SCRATCH.get();
    int newOffset = 0;

    for (int i = 0; i < entryCount; i++) {
      final int oldOffset = slotOffsets[i];
      final int oldSuffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, oldOffset));
      final int valueOffset = oldOffset + 2 + oldSuffixLen;
      final int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));

      final int newSuffixLen = extensionLen + oldSuffixLen;
      final int newEntrySize = 2 + newSuffixLen + 2 + valueLen;

      // Write new entry: [u16 newSuffixLen][extension][oldSuffix][u16 valueLen][value]
      scratch[newOffset] = (byte) (newSuffixLen & 0xFF);
      scratch[newOffset + 1] = (byte) ((newSuffixLen >>> 8) & 0xFF);
      System.arraycopy(extension, 0, scratch, newOffset + 2, extensionLen);
      MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, oldOffset + 2, scratch, newOffset + 2 + extensionLen,
          oldSuffixLen);
      // Copy value length and value bytes
      MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, valueOffset, scratch, newOffset + 2 + newSuffixLen,
          2 + valueLen);

      slotOffsets[i] = newOffset;
      newOffset += newEntrySize;
    }

    // Copy rebuilt entries back to slotMemory
    MemorySegment.copy(scratch, 0, slotMemory, ValueLayout.JAVA_BYTE, 0, newOffset);
    usedSlotMemorySize = newOffset;

    // Update prefix
    commonPrefix = Arrays.copyOf(commonPrefix, newPrefixLen);
    commonPrefixLen = newPrefixLen;

    // Invalidate PEXT index (disc bits over suffixes changed)
    pextValid = false;


  }

  /**
   * Insert entry at specified position. Extracts suffix from full key.
   *
   * @param pos insertion position
   * @param key the full key
   * @param value the value
   * @return true if successful
   */
  private boolean insertAtWithKey(int pos, byte[] key, byte[] value) {
    // Extract suffix
    final int suffixLen = key.length - commonPrefixLen;
    return insertAtSuffix(pos, key, commonPrefixLen, suffixLen, value);
  }

  /**
   * Insert entry at specified position using a suffix slice from a key array.
   *
   * @param pos insertion position
   * @param keyBuf buffer containing the key (suffix starts at suffixOffset)
   * @param suffixOffset offset into keyBuf where suffix starts
   * @param suffixLen length of the suffix
   * @param value the value
   * @return true if successful
   * @throws IllegalArgumentException if suffix or value length exceeds 65535 bytes
   */
  private boolean insertAtSuffix(int pos, byte[] keyBuf, int suffixOffset, int suffixLen, byte[] value) {
    if (suffixLen > MAX_KEY_VALUE_LENGTH) {
      throw new IllegalArgumentException("Suffix length " + suffixLen + " exceeds maximum " + MAX_KEY_VALUE_LENGTH);
    }
    if (value.length > MAX_KEY_VALUE_LENGTH) {
      throw new IllegalArgumentException("Value length " + value.length + " exceeds maximum " + MAX_KEY_VALUE_LENGTH);
    }
    if (entryCount >= MAX_ENTRIES) {
      return false; // Page full, needs split
    }

    ensureMutableSlotMemory();

    // Calculate entry size: [u16 suffixLen][suffix][u16 valueLen][value]
    final int entrySize = 2 + suffixLen + 2 + value.length;

    if (usedSlotMemorySize + entrySize > slotMemory.byteSize()) {
      compact();
      if (usedSlotMemorySize + entrySize > slotMemory.byteSize()) {
        return false;
      }
    }

    // Shift offsets to make room
    if (pos < entryCount) {
      System.arraycopy(slotOffsets, pos, slotOffsets, pos + 1, entryCount - pos);
    }

    // Write entry to slotMemory
    int offset = usedSlotMemorySize;
    slotOffsets[pos] = offset;

    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) suffixLen);
    offset += 2;
    if (suffixLen > 0) {
      MemorySegment.copy(keyBuf, suffixOffset, slotMemory, ValueLayout.JAVA_BYTE, offset, suffixLen);
    }
    offset += suffixLen;
    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) value.length);
    offset += 2;
    if (value.length > 0) {
      MemorySegment.copy(value, 0, slotMemory, ValueLayout.JAVA_BYTE, offset, value.length);
    }

    usedSlotMemorySize += entrySize;
    entryCount++;

    // Invalidate PEXT index
    pextValid = false;

    return true;
  }

  /**
   * Check if the page needs to be split. This returns true if either: - Entry count has reached the
   * maximum, or - Slot memory is nearly full (less than MIN_ENTRY_SPACE remaining)
   *
   * @return true if page is full and needs splitting
   */
  public boolean needsSplit() {
    // Minimum space needed for a typical entry (key + value)
    // This should be conservative enough to trigger splits before insertAt fails
    final int MIN_ENTRY_SPACE = 128; // 2 + 32 (key) + 2 + 92 (value) typical

    if (entryCount >= MAX_ENTRIES) {
      return true;
    }

    // Also need to split if slot memory is nearly full
    long remainingSpace = slotMemory.byteSize() - usedSlotMemorySize;
    return remainingSpace < MIN_ENTRY_SPACE;
  }

  /**
   * Check if this page can be split (has at least 2 entries).
   *
   * <p>
   * A page with only 1 entry cannot be split using the normal splitting algorithm. This typically
   * happens when many identical keys are merged into a single entry with a very large value.
   * </p>
   *
   * @return true if the page can be split (has >= 2 entries)
   */
  public boolean canSplit() {
    return entryCount >= 2;
  }

  /**
   * Check if an entry with the given key and value would fit in this page.
   *
   * @param key the full key bytes
   * @param value the value bytes
   * @return true if the entry would fit
   */
  public boolean canFit(byte[] key, byte[] value) {
    if (entryCount >= MAX_ENTRIES) {
      return false;
    }

    // Suffix = key after common prefix
    final int lcp = longestCommonPrefix(commonPrefix, commonPrefixLen, key, key.length);
    final int suffixLen = key.length - lcp;
    // Calculate entry size: [u16 suffixLen][suffix][u16 valueLen][value]
    final int entrySize = 2 + suffixLen + 2 + value.length;
    // Check against physical capacity — insertAt will auto-compact if needed,
    // so use DEFAULT_SIZE as the upper bound rather than current usedSlotMemorySize
    return entrySize <= slotMemory.byteSize();
  }

  /**
   * Get the remaining space in this page's slot memory.
   *
   * @return remaining space in bytes
   */
  public long getRemainingSpace() {
    return slotMemory.byteSize() - usedSlotMemorySize;
  }

  /**
   * Ensure the slot memory is full-size ({@link #DEFAULT_SIZE}) for mutation.
   * Zero-copy deserialized pages have a slotMemory sized to exactly {@code usedSlotMemorySize},
   * which prevents any new inserts. This method lazily re-allocates to DEFAULT_SIZE,
   * copying existing data, so the page becomes mutable.
   */
  private void ensureMutableSlotMemory() {
    if (slotMemory.byteSize() >= DEFAULT_SIZE) {
      return;
    }
    final MemorySegmentAllocator allocator = Allocators.getInstance();
    final MemorySegment newMemory = allocator.allocate(DEFAULT_SIZE);
    MemorySegment.copy(slotMemory, 0, newMemory, 0, usedSlotMemorySize);
    // Capture old releaser BEFORE reassigning — release old memory AFTER assigning new
    // to prevent use-after-free if allocator.allocate() succeeds but subsequent ops fail
    final Runnable oldReleaser = releaser;
    slotMemory = newMemory;
    final MemorySegment segmentToRelease = newMemory;
    releaser = () -> allocator.release(segmentToRelease);
    // Now safe to release old memory — slotMemory already points to new allocation
    if (oldReleaser != null) {
      oldReleaser.run();
    }
  }

  /**
   * Compact the page by removing fragmentation.
   *
   * <p>
   * When values are updated in place with smaller values, or entries are deleted, the page can become
   * fragmented. This method rebuilds the page with all entries packed contiguously, freeing up space.
   * </p>
   *
   * <p>Uses a thread-local scratch buffer to avoid the 2×n byte[] allocations of the naive approach
   * (which would allocate 1024+ arrays for a full 512-entry page).</p>
   *
   * @return the amount of space reclaimed
   */
  public int compact() {
    if (entryCount == 0) {
      final int reclaimed = usedSlotMemorySize;
      usedSlotMemorySize = 0;
      return reclaimed;
    }

    // Use a thread-local scratch buffer to hold a compacted snapshot of the data.
    // This is O(usedSlotMemorySize) memory touch but only one allocation (the ThreadLocal byte[]).
    byte[] scratch = COMPACT_SCRATCH.get();
    if (scratch.length < usedSlotMemorySize) {
      scratch = new byte[usedSlotMemorySize];
      COMPACT_SCRATCH.set(scratch);
    }

    // Copy all active entries into scratch in sorted (slot) order, recording new offsets.
    int newOffset = 0;
    for (int i = 0; i < entryCount; i++) {
      final int oldOffset = slotOffsets[i];
      final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, oldOffset));
      final int valueLen  = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, oldOffset + 2 + suffixLen));
      final int entrySize = 2 + suffixLen + 2 + valueLen;

      // Bulk-copy the raw entry bytes [u16 suffixLen][suffix][u16 valueLen][value]
      MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, oldOffset, scratch, newOffset, entrySize);
      slotOffsets[i] = newOffset;
      newOffset += entrySize;
    }

    // Bulk-copy compacted data back to slotMemory
    final int oldUsed = usedSlotMemorySize;
    MemorySegment.copy(scratch, 0, slotMemory, ValueLayout.JAVA_BYTE, 0, newOffset);
    usedSlotMemorySize = newOffset;


    return oldUsed - usedSlotMemorySize;
  }

  /**
   * Get entry count.
   *
   * @return number of entries
   */
  public int getEntryCount() {
    return entryCount;
  }

  // ===== Delete operations =====

  /**
   * Tombstone value: single byte 0xFE indicating a logically deleted entry.
   *
   * <p>Tombstones are required for correctness with INCREMENTAL and DIFFERENTIAL
   * versioning: if an entry were physically removed, older fragments would still
   * contain the key and {@code combineHOTLeafPages} would resurrect it. By keeping
   * the key with a tombstone value, the newer fragment "shadows" the older entry
   * and versioning reconstruction correctly skips it.</p>
   */
  private static final byte[] TOMBSTONE_VALUE = {(byte) 0xFE};

  /**
   * Delete an entry by key (tombstone-based).
   *
   * <p>Replaces the value with a tombstone marker ({@code 0xFE}) rather than
   * physically removing the entry. This is essential for versioning correctness:
   * INCREMENTAL and DIFFERENTIAL modes reconstruct pages by merging fragments
   * from newest to oldest. If an entry were physically removed, the older
   * fragment's copy would be resurrected during reconstruction.</p>
   *
   * @param key the key to delete
   * @return true if entry was found and tombstoned, false if not found or already tombstoned
   */
  public boolean delete(byte[] key) {
    Objects.requireNonNull(key);
    final int index = findEntry(key);
    if (index < 0) {
      return false;
    }
    return deleteAt(index);
  }

  /**
   * Delete entry at the given index (tombstone-based).
   *
   * <p>Replaces the value with a tombstone marker rather than physically removing
   * the entry. See {@link #delete(byte[])} for the rationale.</p>
   *
   * @param index the entry index to delete
   * @return true if the entry was tombstoned, false if index is invalid or already a tombstone
   */
  public boolean deleteAt(int index) {
    if (index < 0 || index >= entryCount) {
      return false;
    }

    // Check if already tombstoned
    final byte[] currentValue = getValue(index);
    if (NodeReferencesSerializer.isTombstone(currentValue, 0, currentValue.length)) {
      return false;
    }

    // Write tombstone value instead of physically removing the entry.
    // The key remains in the page so versioning reconstruction sees it and
    // does not resurrect the entry from older fragments.
    return updateValue(index, TOMBSTONE_VALUE);
  }

  // ===== Merge, Update, and Copy operations for HOT index =====

  /**
   * Update the value at a given index.
   *
   * <p>
   * This is used for merging NodeReferences - the new value replaces the old.
   * </p>
   *
   * @param index the entry index
   * @param newValue the new value
   * @return true if updated, false if there wasn't enough space
   */
  public boolean updateValue(int index, byte[] newValue) {
    Objects.checkIndex(index, entryCount);
    Objects.requireNonNull(newValue);
    if (newValue.length > MAX_KEY_VALUE_LENGTH) {
      throw new IllegalArgumentException("Value length " + newValue.length + " exceeds maximum " + MAX_KEY_VALUE_LENGTH);
    }

    // Get old entry info (suffix-based: [u16 suffixLen][suffix][u16 valueLen][value])
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int valueOffset = offset + 2 + suffixLen;
    final int oldValueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));

    // If new value same size or smaller, update in-place
    if (newValue.length <= oldValueLen) {
      slotMemory.set(JAVA_SHORT_UNALIGNED, valueOffset, (short) newValue.length);
      MemorySegment.copy(newValue, 0, slotMemory, ValueLayout.JAVA_BYTE, valueOffset + 2, newValue.length);
      return true;
    }

    // New value is larger - need to append and update offset
    ensureMutableSlotMemory();
    final int newEntrySize = 2 + suffixLen + 2 + newValue.length;
    if (usedSlotMemorySize + newEntrySize > slotMemory.byteSize()) {
      compact();
      if (usedSlotMemorySize + newEntrySize > slotMemory.byteSize()) {
        return false;
      }
    }

    // Copy suffix and new value to end of used space
    final int newOffset = usedSlotMemorySize;

    slotMemory.set(JAVA_SHORT_UNALIGNED, newOffset, (short) suffixLen);
    if (suffixLen > 0) {
      MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, offset + 2, slotMemory, ValueLayout.JAVA_BYTE,
          newOffset + 2, suffixLen);
    }
    slotMemory.set(JAVA_SHORT_UNALIGNED, newOffset + 2 + suffixLen, (short) newValue.length);
    MemorySegment.copy(newValue, 0, slotMemory, ValueLayout.JAVA_BYTE, newOffset + 2 + suffixLen + 2,
        newValue.length);

    slotOffsets[index] = newOffset;
    usedSlotMemorySize += newEntrySize;

    return true;
  }

  /**
   * Merge a value with existing entry using NodeReferences OR semantics.
   *
   * <p>
   * If key exists, merges the NodeReferences (OR operation on bitmaps). If key doesn't exist, inserts
   * new entry.
   * </p>
   *
   * @param key the key bytes
   * @param keyLen the key length
   * @param value the value bytes (serialized NodeReferences)
   * @param valueLen the value length
   * @return true if merged/inserted successfully
   */
  public boolean mergeWithNodeRefs(byte[] key, int keyLen, byte[] value, int valueLen) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);

    // Search for existing key
    final byte[] keySlice = keyLen == key.length
        ? key
        : Arrays.copyOf(key, keyLen);

    // Handle prefix for the incoming key (may shrink prefix)
    handlePrefixForInsert(keySlice);

    int index = findEntry(keySlice);

    if (index >= 0) {
      // Key exists - merge NodeReferences
      final byte[] existingValue = getValue(index);

      // Deserialize both and merge
      var existingRefs = NodeReferencesSerializer.deserialize(existingValue);
      var newRefs = NodeReferencesSerializer.deserialize(value, 0, valueLen);

      // Check for tombstone in new value
      if (!newRefs.hasNodeKeys()) {
        // Tombstone - set empty value
        return updateValue(index, new byte[] {(byte) 0xFE}); // TOMBSTONE_FORMAT
      }

      // Merge bitmaps (OR operation)
      NodeReferencesSerializer.merge(existingRefs, newRefs);

      // Serialize merged result
      final byte[] mergedBytes = NodeReferencesSerializer.serialize(existingRefs);
      return updateValue(index, mergedBytes);
    } else {
      // Key doesn't exist - insert new entry
      final byte[] valueSlice = valueLen == value.length
          ? value
          : Arrays.copyOf(value, valueLen);
      final int insertPos = -(index + 1);

      return insertAtWithKey(insertPos, keySlice, valueSlice);
    }
  }

  /**
   * Create a deep copy of this page for COW (Copy-on-Write).
   *
   * <p>
   * The copy has its own off-heap memory segment and independent state.
   * </p>
   *
   * @return a new HOTLeafPage with copied data
   */
  public HOTLeafPage copy() {
    // Allocate new off-heap memory
    final MemorySegmentAllocator allocator = Allocators.getInstance();
    final MemorySegment newSlotMemory = allocator.allocate(DEFAULT_SIZE);
    final Runnable newReleaser = () -> allocator.release(newSlotMemory);

    // Bulk copy off-heap data
    MemorySegment.copy(slotMemory, 0, newSlotMemory, 0, usedSlotMemorySize);

    // Deep copy on-heap arrays
    final int[] newSlotOffsets = Arrays.copyOf(slotOffsets, slotOffsets.length);

    // Deep copy prefix
    final byte[] newPrefix = commonPrefixLen > 0 ? Arrays.copyOf(commonPrefix, commonPrefixLen) : EMPTY_PREFIX;

    // Create new page with copied data
    final HOTLeafPage copy = new HOTLeafPage(recordPageKey, revision, indexType, newSlotMemory, newReleaser,
        newSlotOffsets, entryCount, usedSlotMemorySize, newPrefix, commonPrefixLen);

    // Deep copy page references (for overflow entries)
    for (var entry : pageReferences.long2ObjectEntrySet()) {
      copy.pageReferences.put(entry.getLongKey(), entry.getValue());
    }

    return copy;
  }

  /**
   * Split this page, moving the right half of entries to another page.
   *
   * <p>
   * After split:
   * </p>
   * <ul>
   * <li>This page keeps entries [0, splitPoint)</li>
   * <li>Target page gets entries [splitPoint, entryCount)</li>
   * </ul>
   *
   * <p>
   * <b>Edge Case Handling:</b> When the page has only 1 entry (common with many identical keys that
   * get merged), this method returns {@code null} instead of throwing an exception. The caller should
   * handle this by using overflow pages or other strategies.
   * </p>
   *
   * @param target the page to receive the right half of entries
   * @return the first key in the target page (split key for parent navigation), or {@code null} if
   *         the page cannot be split (e.g., only 1 entry)
   */
  public @Nullable byte[] splitTo(HOTLeafPage target) {
    Objects.requireNonNull(target);

    if (entryCount < 2) {
      return null;
    }

    // Split at midpoint
    int splitPoint = entryCount / 2;

    // Copy right half to target using full keys (target will establish its own prefix)
    for (int i = splitPoint; i < entryCount; i++) {
      final byte[] key = getKey(i);
      final byte[] value = getValue(i);

      if (!target.put(key, value) && target.entryCount == 0) {
        // Couldn't even insert one entry
        return null;
      }
    }

    if (target.entryCount == 0) {
      return null;
    }

    // Get the split key (first full key in target)
    final byte[] splitKey = target.getKey(0);

    // Truncate this page to keep only left half
    entryCount = splitPoint;
    recalculateUsedMemory();

    // Recompute prefix for the remaining left half (may have a longer prefix now)
    recomputePrefix();

    // Invalidate PEXT
    pextValid = false;

    return splitKey;
  }

  /**
   * Split this page AND insert the new key+value atomically using MSDB-aware splitting.
   *
   * <p>
   * Computes the most significant discriminative bit (MSDB) including the new key's disc bits
   * with its neighbors, splits all entries by that disc bit, and inserts the new key into the
   * correct half. This eliminates re-navigation after a split because the BiNode's disc bit
   * (computed from boundary keys after this method) is guaranteed to correctly route all keys.
   * </p>
   *
   * <p>
   * Matches the C++ reference implementation's atomic split+insert approach (Binna's thesis),
   * adapted for multi-entry leaf pages.
   * </p>
   *
   * @param target the empty target page to receive the right half
   * @param key the new key to insert
   * @param keyLen the key length
   * @param value the new value to insert
   * @param valueLen the value length
   * @return {@code true} if the split+insert succeeded, {@code false} if it failed
   */
  public boolean splitToWithInsert(HOTLeafPage target, byte[] key, int keyLen,
      byte[] value, int valueLen) {
    Objects.requireNonNull(target);

    final int count = entryCount;
    if (count < 1) {
      return false;
    }

    // Slice key/value to actual length
    final byte[] keySlice = keyLen == key.length ? key : Arrays.copyOf(key, keyLen);
    final byte[] valueSlice = valueLen == value.length ? value : Arrays.copyOf(value, valueLen);

    // Use full keys for MSDB computation (disc bits are absolute positions)
    final int searchResult = findEntry(keySlice);
    final boolean isNew = searchResult < 0;
    final int insertPos = isNew ? -(searchResult + 1) : searchResult;

    // Compute MSDB including the new key's disc bits with its neighbors
    final int msdb = isNew ? findMsdbWithNewKey(keySlice, insertPos) : findMsdbBit();

    if (msdb < 0) {
      return false; // All keys identical — can't split
    }

    // Find split point: first existing key with bit msdb = 1
    int splitPoint = count;
    for (int i = 0; i < count; i++) {
      if (DiscriminativeBitComputer.isBitSet(getKeySlice(i), msdb)) {
        splitPoint = i;
        break;
      }
    }

    // Copy right-half entries to target using full keys (target establishes its own prefix)
    int transferred = 0;
    for (int i = splitPoint; i < count; i++) {
      final byte[] k = getKey(i);
      final byte[] v = getValue(i);
      // Use put() which handles prefix establishment
      if (!target.put(k, v) && target.entryCount == 0) {
        break;
      }
      transferred++;
    }

    final int expectedTransfers = count - splitPoint;
    if (transferred < expectedTransfers) {
      target.entryCount = 0;
      target.usedSlotMemorySize = 0;
      target.commonPrefix = EMPTY_PREFIX;
      target.commonPrefixLen = 0;
      return false;
    }

    // Save source state before truncation
    final int savedEntryCount = entryCount;
    final int savedUsedMemory = usedSlotMemorySize;
    final byte[] savedPrefix = commonPrefix;
    final int savedPrefixLen = commonPrefixLen;

    // Truncate self to left half. Do NOT call recomputePrefix() yet — it may rewrite
    // slotMemory/slotOffsets if the prefix grows, which makes rollback impossible.
    // The old (shorter) prefix is still a valid prefix of all remaining keys, so
    // mergeWithNodeRefs will produce correct but slightly longer suffixes.
    entryCount = splitPoint;
    recalculateUsedMemory();

    // Insert/update the key in the correct half based on disc bit
    final boolean insertOk;
    if (DiscriminativeBitComputer.isBitSet(keySlice, msdb)) {
      insertOk = target.mergeWithNodeRefs(keySlice, keySlice.length, valueSlice, valueSlice.length);
    } else {
      insertOk = mergeWithNodeRefs(keySlice, keySlice.length, valueSlice, valueSlice.length);
    }

    if (!insertOk || entryCount == 0 || target.entryCount == 0) {
      // Restore source page — safe because slotMemory/slotOffsets were not modified
      entryCount = savedEntryCount;
      usedSlotMemorySize = savedUsedMemory;
      commonPrefix = savedPrefix;
      commonPrefixLen = savedPrefixLen;
      pextValid = false;
      // Clear target
      target.entryCount = 0;
      target.usedSlotMemorySize = 0;
      target.commonPrefix = EMPTY_PREFIX;
      target.commonPrefixLen = 0;
      return false;
    }

    // Success — now safe to recompute prefix with all entries (including the inserted key)
    recomputePrefix();
    pextValid = false;
    return true;
  }

  /**
   * Compute the MSDB bit position across all existing adjacent key pairs.
   *
   * @return the most significant disc bit, or -1 if no discriminative bit exists
   */
  private int findMsdbBit() {
    if (entryCount < 2) {
      return -1;
    }

    int bestBit = Integer.MAX_VALUE;
    for (int i = 0; i < entryCount - 1; i++) {
      final int bit = DiscriminativeBitComputer.computeDifferingBit(getKeySlice(i), getKeySlice(i + 1));
      if (bit >= 0 && bit < bestBit) {
        bestBit = bit;
      }
    }
    return bestBit == Integer.MAX_VALUE ? -1 : bestBit;
  }

  /**
   * Compute the MSDB including a new key that would be inserted at {@code insertPos}.
   *
   * <p>
   * Scans all adjacent pairs in the virtual sorted array (existing keys with the new key
   * inserted), returning the most significant disc bit. The original pair
   * {@code (keys[insertPos-1], keys[insertPos])} is replaced by
   * {@code (keys[insertPos-1], newKey)} and {@code (newKey, keys[insertPos])}.
   * </p>
   *
   * @param newKey the new key being inserted
   * @param insertPos the position where the new key would be inserted
   * @return the most significant disc bit, or -1 if none found
   */
  private int findMsdbWithNewKey(byte[] newKey, int insertPos) {
    final int count = entryCount;
    int bestBit = Integer.MAX_VALUE;

    // Check all original adjacent pairs, skipping the one broken by insertion
    for (int i = 0; i < count - 1; i++) {
      if (insertPos > 0 && insertPos < count && i == insertPos - 1) {
        continue; // This pair is replaced by K's two new pairs
      }
      final int bit = DiscriminativeBitComputer.computeDifferingBit(getKeySlice(i), getKeySlice(i + 1));
      if (bit >= 0 && bit < bestBit) {
        bestBit = bit;
      }
    }

    // Check new key's predecessor pair (mixed: MemorySegment vs byte[])
    if (insertPos > 0) {
      final int bit = DiscriminativeBitComputer.computeDifferingBit(getKeySlice(insertPos - 1), newKey);
      if (bit >= 0 && bit < bestBit) {
        bestBit = bit;
      }
    }

    // Check new key's successor pair (mixed: byte[] vs MemorySegment)
    if (insertPos < count) {
      final int bit = DiscriminativeBitComputer.computeDifferingBit(getKeySlice(insertPos), newKey);
      if (bit >= 0 && bit < bestBit) {
        bestBit = bit;
      }
    }

    return bestBit == Integer.MAX_VALUE ? -1 : bestBit;
  }

  /**
   * Recalculate the used slot memory size based on actual entries. Called after split to allow new
   * inserts on the truncated page.
   *
   * <p>Uses {@link Short#toUnsignedInt} to handle lengths up to 65535 correctly.</p>
   */
  private void recalculateUsedMemory() {
    if (entryCount == 0) {
      usedSlotMemorySize = 0;
      return;
    }

    int maxEndOffset = 0;
    for (int i = 0; i < entryCount; i++) {
      final int offset = slotOffsets[i];
      final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
      final int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset + 2 + suffixLen));
      final int endOffset = offset + 2 + suffixLen + 2 + valueLen;
      if (endOffset > maxEndOffset) {
        maxEndOffset = endOffset;
      }
    }
    usedSlotMemorySize = maxEndOffset;
  }

  /**
   * Recompute the common prefix after a split or truncation. After removing entries,
   * the remaining entries may share a longer common prefix. This method computes the
   * new LCP of all remaining entries' full keys and rebuilds entries with shorter suffixes
   * if the prefix grew.
   *
   * <p>The prefix can only GROW after truncation (not shrink), because removing entries
   * can only increase the LCP. Growing the prefix means shorter suffixes, which saves space.</p>
   */
  private void recomputePrefix() {
    if (entryCount == 0) {
      commonPrefix = EMPTY_PREFIX;
      commonPrefixLen = 0;
      return;
    }

    if (entryCount == 1) {
      // Single entry: prefix = full key, suffix = empty
      final byte[] fullKey = getKey(0);
      final int oldPrefixLen = commonPrefixLen;
      commonPrefix = fullKey;
      commonPrefixLen = fullKey.length;

      if (commonPrefixLen != oldPrefixLen) {
        // Rewrite the single entry with empty suffix
        ensureMutableSlotMemory();
        // Read value BEFORE changing slotOffsets — getValue uses slotOffsets[0] to locate the entry
        final byte[] value = getValue(0);
        slotOffsets[0] = 0;
        slotMemory.set(JAVA_SHORT_UNALIGNED, 0, (short) 0); // suffixLen = 0
        slotMemory.set(JAVA_SHORT_UNALIGNED, 2, (short) value.length);
        if (value.length > 0) {
          MemorySegment.copy(value, 0, slotMemory, ValueLayout.JAVA_BYTE, 4, value.length);
        }
        usedSlotMemorySize = 4 + value.length;
      }
      pextValid = false;
      return;
    }

    // Compute LCP of first and last full keys (since entries are sorted, LCP of
    // first and last is the LCP of ALL entries)
    final byte[] firstKey = getKey(0);
    final byte[] lastKey = getKey(entryCount - 1);
    final int newPrefixLen = longestCommonPrefix(firstKey, firstKey.length, lastKey, lastKey.length);

    if (newPrefixLen > commonPrefixLen) {
      // Prefix grew — rebuild entries with shorter suffixes
      final byte[] newPrefix = Arrays.copyOf(firstKey, newPrefixLen);
      final int extensionLen = newPrefixLen - commonPrefixLen;

      ensureMutableSlotMemory();
      final byte[] scratch = COMPACT_SCRATCH.get();
      int newOffset = 0;

      for (int i = 0; i < entryCount; i++) {
        final int oldOffset = slotOffsets[i];
        final int oldSuffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, oldOffset));
        final int valueOffset = oldOffset + 2 + oldSuffixLen;
        final int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));

        // New suffix = old suffix minus the extension bytes at the start
        final int newSuffixLen = oldSuffixLen - extensionLen;
        final int newEntrySize = 2 + newSuffixLen + 2 + valueLen;

        scratch[newOffset] = (byte) (newSuffixLen & 0xFF);
        scratch[newOffset + 1] = (byte) ((newSuffixLen >>> 8) & 0xFF);
        if (newSuffixLen > 0) {
          MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, oldOffset + 2 + extensionLen,
              scratch, newOffset + 2, newSuffixLen);
        }
        // Copy value length + value
        MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, valueOffset, scratch,
            newOffset + 2 + newSuffixLen, 2 + valueLen);

        slotOffsets[i] = newOffset;
        newOffset += newEntrySize;
      }

      MemorySegment.copy(scratch, 0, slotMemory, ValueLayout.JAVA_BYTE, 0, newOffset);
      usedSlotMemorySize = newOffset;
      commonPrefix = newPrefix;
      commonPrefixLen = newPrefixLen;
    }

    pextValid = false;

  }

  /**
   * Get the first (minimum) key in this page.
   *
   * @return the first key, or empty array if page is empty (never null)
   */
  public byte[] getFirstKey() {
    if (entryCount == 0) {
      return new byte[0];
    }
    return getKey(0);
  }

  /**
   * Get the last (maximum) key in this page.
   *
   * @return the last key, or empty array if page is empty (never null)
   */
  public byte[] getLastKey() {
    if (entryCount == 0) {
      return new byte[0];
    }
    return getKey(entryCount - 1);
  }

  /**
   * Get all keys in this page as an array.
   *
   * <p>
   * Keys are returned in sorted order.
   * </p>
   *
   * @return array of all keys (never null, may be empty)
   */
  public byte[][] getAllKeys() {
    byte[][] keys = new byte[entryCount][];
    for (int i = 0; i < entryCount; i++) {
      keys[i] = getKey(i);
    }
    return keys;
  }

  /**
   * Merge another HOTLeafPage into this one.
   *
   * <p>
   * Used for versioning - combines entries from multiple page fragments. Newer entries take
   * precedence. NodeReferences are OR-merged.
   * </p>
   *
   * @param other the page to merge from
   * @return true if all entries merged successfully
   */
  public boolean mergeFrom(HOTLeafPage other) {
    Objects.requireNonNull(other);

    for (int i = 0; i < other.entryCount; i++) {
      byte[] key = other.getKey(i);
      byte[] value = other.getValue(i);

      if (!mergeWithNodeRefs(key, key.length, value, value.length)) {
        return false; // Page full
      }
    }
    return true;
  }

  // ===== Guard-based lifetime management =====

  /**
   * Acquire a guard (increment reference count). Pages with guards cannot be evicted.
   *
   * @return true if the guard was acquired, false if the page is already closed or orphaned
   */
  public boolean acquireGuard() {
    if (closed.get() || isOrphaned) {
      return false;
    }
    guardCount.incrementAndGet();
    // Double-check after increment to prevent acquire-after-close race
    if (closed.get()) {
      guardCount.decrementAndGet();
      return false;
    }
    hot = true;
    return true;
  }

  /**
   * Release a guard (decrement reference count). If orphaned and guard count reaches zero, close the
   * page. Uses atomic close-once to prevent TOCTOU race between releaseGuard and markOrphaned.
   */
  public void releaseGuard() {
    final int remaining = guardCount.decrementAndGet();
    if (remaining < 0) {
      throw new IllegalStateException("Guard count underflow for page " + recordPageKey);
    }
    if (remaining == 0 && isOrphaned) {
      if (closed.compareAndSet(false, true)) {
        releaseMemory();
      }
    }
  }

  /**
   * Get current guard count.
   *
   * @return guard count
   */
  public int getGuardCount() {
    return guardCount.get();
  }

  /**
   * Mark page as orphaned (removed from cache but still guarded).
   * Uses atomic close-once to prevent TOCTOU race between releaseGuard and markOrphaned.
   */
  public void markOrphaned() {
    isOrphaned = true;
    if (guardCount.get() == 0) {
      if (closed.compareAndSet(false, true)) {
        releaseMemory();
      }
    }
  }

  /**
   * Check if page is orphaned.
   *
   * @return true if orphaned
   */
  public boolean isOrphaned() {
    return isOrphaned;
  }

  /**
   * Close the page and release off-heap memory. Thread-safe; only the first call
   * actually releases the memory segment.
   */
  public void close() {
    if (closed.compareAndSet(false, true)) {
      releaseMemory();
    }
  }

  /**
   * Release off-heap memory. Must only be called once — callers must ensure
   * single-call semantics via the {@link #closed} atomic flag.
   */
  private void releaseMemory() {
    if (releaser != null) {
      releaser.run();
    }
  }

  // ===== KeyValuePage interface implementation =====

  @Override
  public long getPageKey() {
    return recordPageKey;
  }

  @Override
  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public int getRevision() {
    return revision;
  }

  @Override
  public int size() {
    return entryCount;
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public MemorySegment slots() {
    return slotMemory;
  }

  @Override
  public int getUsedSlotsSize() {
    return usedSlotMemorySize;
  }

  @Override
  public MemorySegment getSlot(int slotNumber) {
    if (slotNumber < 0 || slotNumber >= entryCount) {
      return null;
    }
    final int offset = slotOffsets[slotNumber];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int valueOffset = offset + 2 + suffixLen;
    final int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));
    final int totalLen = 2 + suffixLen + 2 + valueLen;
    return slotMemory.asSlice(offset, totalLen);
  }

  @Override
  public byte[] getSlotAsByteArray(int slotNumber) {
    MemorySegment slot = getSlot(slotNumber);
    if (slot == null) {
      return null;
    }
    byte[] data = new byte[(int) slot.byteSize()];
    MemorySegment.copy(slot, ValueLayout.JAVA_BYTE, 0, data, 0, data.length);
    return data;
  }

  @Override
  public void setSlot(MemorySegment data, int slotNumber) {
    // For HOTLeafPage, this is handled by put()
    throw new UnsupportedOperationException("Use put() for HOTLeafPage");
  }

  @Override
  public void setSlot(byte[] recordData, int offset) {
    // For HOTLeafPage, this is handled by put()
    throw new UnsupportedOperationException("Use put() for HOTLeafPage");
  }

  @Override
  public MemorySegment deweyIds() {
    return null; // HOTLeafPage doesn't use Dewey IDs
  }

  @Override
  public int getUsedDeweyIdSize() {
    return 0;
  }

  @Override
  public byte[] getDeweyIdAsByteArray(int offset) {
    return null;
  }

  @Override
  public MemorySegment getDeweyId(int offset) {
    return null;
  }

  @Override
  public void setDeweyId(byte[] deweyId, int offset) {
    // Not supported
  }

  @Override
  public void setDeweyId(MemorySegment deweyId, int offset) {
    // Not supported
  }

  @Override
  public void setRecord(DataRecord record) {
    throw new UnsupportedOperationException("HOTLeafPage uses put() instead of setRecord()");
  }

  @Override
  public DataRecord[] records() {
    return null; // HOTLeafPage stores raw key-value pairs, not DataRecords
  }

  @Override
  public DataRecord getRecord(int offset) {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <I extends Iterable<DataRecord>> I values() {
    // Return empty list - HOTLeafPage stores raw key-value pairs
    List<DataRecord> emptyList = Collections.emptyList();
    return (I) emptyList;
  }

  @Override
  public void setPageReference(long key, PageReference reference) {
    pageReferences.put(key, reference);
  }

  @Override
  public PageReference getPageReference(long key) {
    return pageReferences.get(key);
  }

  @Override
  public Set<Map.Entry<Long, PageReference>> referenceEntrySet() {
    // Convert fastutil entry set to standard Set<Map.Entry>
    Set<Map.Entry<Long, PageReference>> result = new HashSet<>();
    for (var entry : pageReferences.long2ObjectEntrySet()) {
      result.add(Map.entry(entry.getLongKey(), entry.getValue()));
    }
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends KeyValuePage<DataRecord>> C newInstance(long recordPageKey,
      IndexType indexType, StorageEngineReader storageEngineReader) {
    return (C) new HOTLeafPage(recordPageKey, storageEngineReader.getRevisionNumber(), indexType);
  }

  // ===== Page interface =====

  @Override
  public List<PageReference> getReferences() {
    // HOTLeafPage doesn't have child page references in the traditional sense
    // Return the overflow page references
    return new ArrayList<>(pageReferences.values());
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    return null; // HOTLeafPage doesn't have child references
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    return false;
  }

  // ===== Prefix compression accessors =====

  /**
   * Get the common prefix shared by all keys in this page.
   *
   * @return the common prefix (never null, may be empty)
   */
  public byte[] getCommonPrefix() {
    return commonPrefix;
  }

  /**
   * Get the common prefix length.
   *
   * @return the length of the common prefix
   */
  public int getCommonPrefixLen() {
    return commonPrefixLen;
  }

  /**
   * Get the slot offset for the given entry index (for serialization).
   *
   * @param index the entry index
   * @return the offset into slotMemory
   */
  public int getSlotOffset(int index) {
    return slotOffsets[index];
  }

  // ===== Utility methods =====

  /**
   * Get the hot flag for clock-based eviction.
   *
   * @return true if recently accessed
   */
  public boolean isHot() {
    return hot;
  }

  /**
   * Clear the hot flag (called by clock sweeper).
   */
  public void clearHot() {
    hot = false;
  }

  /**
   * Increment version (called when page is reused).
   *
   * @return new version
   */
  public int incrementVersion() {
    return version.incrementAndGet();
  }

  /**
   * Get current version.
   *
   * @return version number
   */
  public int getVersion() {
    return version.get();
  }

  @Override
  public String toString() {
    return "HOTLeafPage{" + "pageKey=" + recordPageKey + ", revision=" + revision + ", indexType=" + indexType
        + ", entryCount=" + entryCount + ", usedSlotMemorySize=" + usedSlotMemorySize + ", guardCount="
        + guardCount.get() + ", closed=" + closed.get() + '}';
  }
}

