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

import io.sirix.node.LE;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.settings.Constants;
import io.sirix.cache.Allocators;
import io.sirix.index.hot.DiscriminativeBitComputer;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.index.hot.PathKeySerializer;
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
import java.util.function.IntConsumer;

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
public final class HOTLeafPage implements KeyValuePage<DataRecord>, io.sirix.cache.CacheablePage {

  /** Sentinel value for "not found" in binary search. */
  public static final int NOT_FOUND = -1;

  /** Default page size for off-heap allocation (64KB). */
  public static final int DEFAULT_SIZE = 64 * 1024;

  /**
   * Page-envelope flag bit: this leaf serializes a trailing segment-reference section (the
   * side map of {@link ProjectionSegmentPage} references keyed by
   * {@link #segmentRefKey(long, int)} — see docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3).
   */
  public static final byte FLAG_SEGMENT_REFS = 0x01;

  /** Maximum segment id in a side-map composite key (occupies the low 8 bits). */
  public static final int MAX_SEGMENT_ID = 0xFF;

  /**
   * THE side-map key convention, in one place so the encoder (projection storage) and the
   * decoder ({@link #moveSegmentRefsAfterSplit}) cannot drift: a side-map entry's key is
   * {@code (ownerSlotKey << 8) | segmentId}, where {@code ownerSlotKey} is the long whose
   * {@code PathKeySerializer} encoding is the owning slot's stored key bytes. Validates both
   * halves — a truncated owner key would collide two distinct owners and mis-route refs after
   * splits (sign-extended {@code >> 8} recovery), so it fails loudly here instead.
   *
   * @throws IllegalArgumentException when {@code segmentId} is outside [0, 255] or
   *         {@code ownerSlotKey} does not survive the {@code << 8 >> 8} round-trip
   *         (|ownerSlotKey| ≥ 2^55)
   */
  public static long segmentRefKey(final long ownerSlotKey, final int segmentId) {
    if (segmentId < 0 || segmentId > MAX_SEGMENT_ID) {
      throw new IllegalArgumentException("segmentId must be in [0, " + MAX_SEGMENT_ID + "]: " + segmentId);
    }
    if ((ownerSlotKey << 8) >> 8 != ownerSlotKey) {
      throw new IllegalArgumentException("ownerSlotKey out of range for the side-map composite encoding"
          + " (|ownerSlotKey| must be < 2^55): " + ownerSlotKey);
    }
    return (ownerSlotKey << 8) | segmentId;
  }

  /** Inverse of {@link #segmentRefKey}: the owning slot's long key. */
  public static long segmentRefOwnerSlot(final long refKey) {
    return refKey >> 8;
  }

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
  private static final ValueLayout.OfShort JAVA_SHORT_UNALIGNED = LE.SHORT;

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

  // ===== Slot-granular CoW state (mirrors KeyValueLeafPage.preservationBitmap) =====
  // Each bit at position i in dirtyBitmap tracks whether entry index i has been mutated since
  // copy(). 8 longs cover MAX_ENTRIES = 512 indices. Insert/delete shift the bitmap with the
  // same arraycopy direction/length as slotOffsets so bits track entries by INDEX within the
  // leaf's lifetime; cross-revision merge happens by KEY in combineHOTLeafPages, not by index.
  private static final int DIRTY_BITMAP_WORDS = MAX_ENTRIES >>> 6;
  private final long[] dirtyBitmap = new long[DIRTY_BITMAP_WORDS];

  // Reference to the complete page from which non-dirty entries can be lazily materialized at
  // serialize-time when SLIDING_SNAPSHOT (or other strategies) require a full-fragment emit.
  // copy() sets this on the new instance and clears the bitmap; the writer never touches this
  // directly. Acts as the HOT analogue of KeyValueLeafPage.completePageRef.
  private @Nullable HOTLeafPage completePageRef;

  // Set after a leaf split: signals that this page contains ALL entries for its page key
  // (i.e., a complete snapshot, not a delta). The combining logic in mergeHOTFragmentsByKey
  // must NOT add entries from older fragments when this flag is set, otherwise entries that
  // were moved to the right-half page during the split get resurrected from the base revision.
  private boolean completeDump;

  // ===== Phase 7a — leaf-owned-bits metadata =====
  // Tracks which absolute MSB-first bit positions are captured by ANY ancestor mask on the
  // path that descends to this leaf. Keys in this leaf MUST have a constant value at each
  // owned bit (0 or 1 matching ancestorOwnedValues). This metadata enables β-constancy
  // enforcement at insert time: callers can check whether inserting a key would violate the
  // constancy invariant BEFORE actually merging.
  //
  // Empty arrays = no constraints (= legacy multi-entry leaf with arbitrary bit values).
  // Sorted ascending (= MSB-first order). Set by splitLeafOnBit / handleLeafSplitAndInsert.
  //
  // ancestorOwnedValues[i] = 0 means bit ancestorOwnedBits[i] is constant 0 across all keys.
  // ancestorOwnedValues[i] = 1 means bit ancestorOwnedBits[i] is constant 1 across all keys.
  private int[] ancestorOwnedBits = EMPTY_BITS;
  private byte[] ancestorOwnedValues = EMPTY_VALUES;
  private static final int[] EMPTY_BITS = new int[0];
  private static final byte[] EMPTY_VALUES = new byte[0];

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
      // Zero-alloc: pass both suffix regions (offset+length) within slotMemory directly to
      // DiscriminativeBitComputer instead of materializing two NativeMemorySegmentImpl views
      // via asSlice. The slot table already validated these offsets at deserialization.
      final int off1 = slotOffsets[i];
      final int len1 = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, off1));
      final int off2 = slotOffsets[i + 1];
      final int len2 = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, off2));
      final int diffBit = DiscriminativeBitComputer.computeDifferingBit(
          slotMemory, off1 + 2L, len1, off2 + 2L, len2);
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

    // Build BE word bit mask: for each disc bit position, compute its long bit position in the
    // 8-byte BE word starting at {@code discBytePos}. BE: byte at window-position {@code p}
    // occupies long bits {@code (7-p)*8 .. (7-p)*8+7}; within that slot, MSB-first bit-in-byte
    // {@code b} is at long bit {@code (7-p)*8 + (7-b) = 63 - (p*8 + b)}. Formula:
    // {@code longBit = 63 - inWindowAbsBit}.
    long mask = 0;
    for (int i = 0; i < entryCount - 1; i++) {
      if (diffBits[i] >= 0) {
        final int absBitPos = diffBits[i]; // byte*8 + bitInByte (MSB-first)
        final int bytePos = absBitPos / 8;
        final int bitInByte = absBitPos % 8; // 0=MSB, 7=LSB
        final int inWindowAbsBit = (bytePos - discBytePos) * 8 + bitInByte;
        mask |= 1L << (63 - inWindowAbsBit);
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
   *
   * <p>Zero-allocation: reads {@code slotMemory} directly via {@code (offset, length)} instead of
   * materializing a {@code MemorySegment.asSlice} view per call. Called once per entry during
   * {@link #buildPextIndex}, so removing the slice eliminates {@code entryCount} view allocations
   * per deserialized leaf page.</p>
   */
  private int computeSuffixPartialKey(int index) {
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    return computePartialKeyFromSegmentRegion(slotMemory, offset + 2L, suffixLen, discBytePos, discBitMask);
  }

  /**
   * Compute PEXT partial key from a byte-range within a MemorySegment.
   *
   * <p>Zero-allocation counterpart to {@link #computePartialKeyFromSegment} that avoids the
   * intermediate {@code asSlice} wrapper.</p>
   */
  private static int computePartialKeyFromSegmentRegion(
      MemorySegment seg, long suffixStart, int suffixLen, int bytePos, long bitMask) {
    final long suffixWord = loadWordBE(seg, suffixStart, suffixLen, bytePos);
    return (int) Long.compress(suffixWord, bitMask);
  }

  /**
   * Compute PEXT partial key from a MemorySegment suffix.
   */
  private static int computePartialKeyFromSegment(MemorySegment suffix, int bytePos, long bitMask) {
    final long suffixWord = loadWordBE(suffix, bytePos);
    return (int) Long.compress(suffixWord, bitMask);
  }

  /**
   * Compute PEXT partial key from a byte array suffix (starting at given offset).
   */
  private static int computePartialKeyFromArray(byte[] key, int suffixOffset, int bytePos, long bitMask) {
    final long suffixWord = loadWordBEFromArray(key, suffixOffset + bytePos);
    return (int) Long.compress(suffixWord, bitMask);
  }

  /**
   * Load up to 8 bytes from a MemorySegment in BE word layout: byte at {@code pos} → long bits
   * 56-63, {@code pos+1} → 48-55, ..., {@code pos+7} → 0-7. Matches
   * {@link io.sirix.page.HOTIndirectPage#getKeyWordAt} for PEXT consistency.
   */
  private static long loadWordBE(MemorySegment segment, int pos) {
    long result = 0;
    final long segLen = segment.byteSize();
    final int end = (int) Math.min(pos + 8, segLen);
    for (int i = pos; i < end; i++) {
      result |= ((long) (segment.get(ValueLayout.JAVA_BYTE, i) & 0xFF)) << ((7 - (i - pos)) * 8);
    }
    return result;
  }

  /**
   * Load up to 8 bytes from a logical byte-range within {@code segment} in BE word layout.
   *
   * <p>The {@code (regionStart, regionLen)} pair carries the implicit slice the legacy
   * {@link #loadWordBE(MemorySegment, int)} reads via {@link MemorySegment#byteSize()}. Equivalent
   * semantics but allocation-free at call sites that own the start/length explicitly
   * (e.g. {@link #computeSuffixPartialKey}).</p>
   *
   * @param segment    underlying memory
   * @param regionStart absolute byte offset of the logical key region within {@code segment}
   * @param regionLen   length of the logical key region in bytes
   * @param pos         byte offset within the region (0-based) to start reading from
   * @return the up-to-8-byte word in BE layout, zero-padded if the region is shorter than 8 bytes
   *         from {@code pos}
   */
  private static long loadWordBE(MemorySegment segment, long regionStart, int regionLen, int pos) {
    long result = 0;
    final int end = Math.min(pos + 8, regionLen);
    for (int i = pos; i < end; i++) {
      result |= ((long) (segment.get(ValueLayout.JAVA_BYTE, regionStart + i) & 0xFF)) << ((7 - (i - pos)) * 8);
    }
    return result;
  }

  /**
   * Load up to 8 bytes from a byte array in BE word layout.
   */
  private static long loadWordBEFromArray(byte[] key, int pos) {
    long result = 0;
    final int end = Math.min(pos + 8, key.length);
    for (int i = pos; i < end; i++) {
      result |= ((long) (key[i] & 0xFF)) << ((7 - (i - pos)) * 8);
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
    final long segSize = slotMemory.byteSize();
    if (offset < 0 || offset + 2 > segSize) {
      return MemorySegment.NULL.reinterpret(0);
    }
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int valueOffset = offset + 2 + suffixLen;
    if (valueOffset + 2 > segSize) {
      return MemorySegment.NULL.reinterpret(0);
    }
    final int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));
    if (valueOffset + 2 + valueLen > segSize) {
      return MemorySegment.NULL.reinterpret(0);
    }
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
    if (offset < 0 || offset + 2 > slotMemory.byteSize()) {
      return null;
    }
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    if (offset + 2 + suffixLen > slotMemory.byteSize()) {
      return null;
    }
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
   * Zero-alloc lexicographic comparison of the key at {@code index} against
   * {@code bound} (treated as an unsigned byte sequence). Returns the usual
   * tri-value: {@code <0} if key&lt;bound, {@code 0} if equal, {@code >0} if
   * key&gt;bound. Avoids the {@code byte[]} allocation that {@link #getKey}
   * and the subsequent {@code MemorySegment.ofArray} wrap incur.
   *
   * <p>Reads the {@code commonPrefix} on-heap bytes first, then the
   * per-entry suffix bytes from off-heap {@code slotMemory}. Lengths use
   * the same {@code suffixLen}-prefix encoding as {@link #getKey}.
   *
   * <p>HFT: used by {@link io.sirix.access.trx.page.HOTRangeCursor} during
   * its per-entry range check to stay allocation-free. The caller holds
   * the leaf guard; the off-heap slot memory is guaranteed live.
   *
   * @param index the entry index (checked)
   * @param bound the upper-bound key (not null)
   * @return a negative, zero, or positive int as specified above
   */
  public int compareKeyWithBound(final int index, final byte[] bound) {
    Objects.checkIndex(index, entryCount);
    Objects.requireNonNull(bound, "bound");
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int keyLen = commonPrefixLen + suffixLen;
    final int minLen = Math.min(keyLen, bound.length);

    // Phase 1: commonPrefix (on-heap byte[]) vs bound[0..commonPrefixLen)
    int p = 0;
    final int commonMin = Math.min(commonPrefixLen, bound.length);
    while (p < commonMin) {
      final int a = commonPrefix[p] & 0xFF;
      final int b = bound[p] & 0xFF;
      if (a != b) return a - b;
      p++;
    }
    // If bound ended within the commonPrefix, the key (which extends via
    // its suffix) is strictly longer → key > bound.
    if (bound.length == commonPrefixLen && suffixLen > 0) return 1;
    if (p == keyLen) {
      // key fully consumed; equal up to keyLen → key < bound if bound longer
      return keyLen - bound.length;
    }

    // Phase 2: suffix (off-heap) vs bound[commonPrefixLen..minLen)
    final int suffixStart = offset + 2;
    while (p < minLen) {
      final int a = slotMemory.get(ValueLayout.JAVA_BYTE, suffixStart + (p - commonPrefixLen)) & 0xFF;
      final int b = bound[p] & 0xFF;
      if (a != b) return a - b;
      p++;
    }
    // Length tie-breaker: longer wins (unsigned lex).
    return keyLen - bound.length;
  }

  /**
   * Zero-alloc most-significant distinguishing bit (MSDB) between the key at {@code index} and
   * {@code other}: the absolute, MSB-first bit position where they first differ, treating bytes
   * beyond either key's length as {@code 0x00}. Identical semantics to
   * {@code HOTBulkBuilder.msdb(byte[], byte[])} but reads this leaf's key in place (on-heap
   * commonPrefix + off-heap suffix) instead of materializing it via {@link #getKey} — used by the
   * incremental-insert descent to score the two insertion-point neighbors without allocating both.
   *
   * @param index the entry index (checked)
   * @param other the other key (not null, and must differ from the key at {@code index})
   * @return the absolute bit index of the first differing bit
   * @throws IllegalStateException if the two keys are equal (no distinguishing bit)
   */
  public int msdbWith(final int index, final byte[] other) {
    Objects.checkIndex(index, entryCount);
    Objects.requireNonNull(other, "other");
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int keyLen = commonPrefixLen + suffixLen;
    final long suffixStart = offset + 2;
    final int max = Math.max(keyLen, other.length);
    for (int i = 0; i < max; i++) {
      final int leafByte = i < commonPrefixLen
          ? (commonPrefix[i] & 0xFF)
          : (i < keyLen
              ? (slotMemory.get(ValueLayout.JAVA_BYTE, suffixStart + (i - commonPrefixLen)) & 0xFF)
              : 0);
      final int otherByte = i < other.length ? (other[i] & 0xFF) : 0;
      final int x = leafByte ^ otherByte;
      if (x != 0) {
        return i * 8 + (Integer.numberOfLeadingZeros(x) - 24);
      }
    }
    throw new IllegalStateException("msdb of equal keys at index " + index);
  }

  /**
   * Zero-alloc little-endian eight-byte decode of the key at {@code index}.
   * Reconstructs the 8-byte composite key from {@code commonPrefix} +
   * {@code slotMemory} suffix bytes in big-endian form, then returns it as
   * a single {@code long}. The projection HOT index uses 8-byte composite
   * keys encoded via {@link io.sirix.index.hot.PathKeySerializer} (sign-flip
   * big-endian), so this method lets callers decode the logical composite
   * without allocating a {@code MemorySegment} wrapper.
   *
   * <p>Caller MUST ensure the key is exactly 8 bytes long (i.e.
   * {@code commonPrefixLen + suffixLen == 8}) before calling — the helper
   * is designed for the projection index's invariant key length and does
   * not defend against shorter/longer keys.
   *
   * @param index the entry index (checked)
   * @return the 8-byte key as an unsigned big-endian long
   */
  public long decodeKey8BE(final int index) {
    Objects.checkIndex(index, entryCount);
    final int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int keyLen = commonPrefixLen + suffixLen;
    if (keyLen != 8) {
      throw new IllegalStateException(
          "decodeKey8BE requires 8-byte keys, got " + keyLen + " (commonPrefixLen="
              + commonPrefixLen + ", suffixLen=" + suffixLen + ") at index=" + index);
    }
    // Assemble high-bytes from commonPrefix, low-bytes from slotMemory suffix.
    long v = 0L;
    for (int i = 0; i < commonPrefixLen; i++) {
      v = (v << 8) | (commonPrefix[i] & 0xFFL);
    }
    final int suffixStart = offset + 2;
    for (int i = 0; i < suffixLen; i++) {
      v = (v << 8) | (slotMemory.get(ValueLayout.JAVA_BYTE, suffixStart + i) & 0xFFL);
    }
    return v;
  }

  /**
   * Get value as byte array (copies data).
   *
   * <p>Zero-intermediate-alloc: reads the value's {@code (offset, length)} directly from
   * {@code slotMemory} and copies into a freshly-allocated {@code byte[]} without going through
   * the {@link #getValueSlice} {@code asSlice} wrapper — the latter would heap-allocate a
   * transient {@code NativeMemorySegmentImpl} view that the copy immediately discards.</p>
   *
   * @param index the entry index
   * @return the value as byte array, or {@code null} when the slot table doesn't address a
   *         readable value (matches the {@link #getValueSlice} {@code NULL} sentinel contract)
   */
  public byte[] getValue(int index) {
    Objects.checkIndex(index, entryCount);
    final int offset = slotOffsets[index];
    final long segSize = slotMemory.byteSize();
    if (offset < 0 || offset + 2 > segSize) {
      return null;
    }
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int valueLenOffset = offset + 2 + suffixLen;
    if (valueLenOffset + 2 > segSize) {
      return null;
    }
    final int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueLenOffset));
    final int valueOffset = valueLenOffset + 2;
    if (valueOffset + valueLen > segSize) {
      return null;
    }
    if (valueLen == 0) {
      return null;
    }
    final byte[] value = new byte[valueLen];
    MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, valueOffset, value, 0, valueLen);
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
      shiftDirtyBitmapForInsert(pos);
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
    markEntryDirty(pos);
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
      shiftDirtyBitmapForInsert(pos);
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
    markEntryDirty(pos);
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
      markEntryDirty(index);
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
      markEntryDirty(index);
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
      shiftDirtyBitmapForInsert(pos);
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
    markEntryDirty(pos);

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
      final int entrySize = Math.addExact(Math.addExact(2 + suffixLen, 2), valueLen);

      // Bulk-copy the raw entry bytes [u16 suffixLen][suffix][u16 valueLen][value]
      MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, oldOffset, scratch, newOffset, entrySize);
      slotOffsets[i] = newOffset;
      newOffset = Math.addExact(newOffset, entrySize);
    }

    // Bulk-copy compacted data back to slotMemory
    final int oldUsed = usedSlotMemorySize;
    MemorySegment.copy(scratch, 0, slotMemory, ValueLayout.JAVA_BYTE, 0, newOffset);
    usedSlotMemorySize = newOffset;
    if (newOffset > oldUsed) {
      throw new IllegalStateException("compact() grew data: " + newOffset + " > " + oldUsed);
    }

    return oldUsed - usedSlotMemorySize;
  }

  /**
   * Physically remove tombstoned entries. After compaction, only active (non-tombstoned)
   * entries remain. The page is marked as a complete dump so fragment combining does not
   * resurrect removed entries from the base revision.
   *
   * @return the number of entries removed
   */
  public int compactTombstones() {
    int activeCount = 0;
    for (int i = 0; i < entryCount; i++) {
      final byte[] value = getValue(i);
      if (value == null) continue;
      if (!NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
        activeCount++;
      }
    }
    if (activeCount == entryCount) {
      return 0;
    }
    final byte[][] activeKeys = new byte[activeCount][];
    final byte[][] activeValues = new byte[activeCount][];
    int idx = 0;
    for (int i = 0; i < entryCount; i++) {
      final byte[] value = getValue(i);
      if (value == null) continue;
      if (!NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
        activeKeys[idx] = getKey(i);
        activeValues[idx] = value;
        idx++;
      }
    }
    final int removed = entryCount - activeCount;
    entryCount = 0;
    usedSlotMemorySize = 0;
    commonPrefix = EMPTY_PREFIX;
    commonPrefixLen = 0;
    clearDirtyBitmap();
    pextValid = false;
    for (int i = 0; i < activeCount; i++) {
      put(activeKeys[i], activeValues[i]);
    }
    recomputePrefix();
    markAllEntriesDirty();
    completeDump = true;
    return removed;
  }

  /**
   * Get entry count.
   *
   * @return number of entries
   */
  public int getEntryCount() {
    return entryCount;
  }

  /**
   * Phase 6a — Check whether bit at MSB-first absolute position {@code absBit} is constant
   * across all keys in this leaf. Returns:
   * <ul>
   *   <li>0 — all keys have bit {@code absBit} = 0</li>
   *   <li>1 — all keys have bit {@code absBit} = 1</li>
   *   <li>-1 — leaf is β-mixed at this bit (= some keys have 0, some have 1) OR leaf is empty</li>
   * </ul>
   *
   * <p>O(N) scan over leaf entries. Keys past their length contribute 0 to the bit lookup
   * (= bit position beyond key length is treated as 0).
   *
   * <p>Used by Phase 5 / Phase 6 helpers in HOTTrieWriter to determine whether extending
   * an ancestor's mask with bit {@code absBit} would preserve β-constancy at this leaf
   * without splitting.
   */
  public int isBitConstantAtAbsBit(int absBit) {
    if (absBit < 0 || entryCount == 0) return -1;
    boolean seen0 = false;
    boolean seen1 = false;
    for (int i = 0; i < entryCount; i++) {
      final byte[] key = getKey(i);
      if (key == null) continue;
      final int bytePos = absBit / 8;
      final int bitInByte = absBit % 8;
      final boolean bitSet = (bytePos < key.length)
          && ((key[bytePos] & (1 << (7 - bitInByte))) != 0);
      if (bitSet) seen1 = true;
      else seen0 = true;
      if (seen0 && seen1) return -1;
    }
    if (seen1 && !seen0) return 1;
    if (seen0 && !seen1) return 0;
    return -1;
  }

  // ===== Phase 7a — owned-bits metadata API =====

  /**
   * Set the leaf's ancestor-owned bits (= absolute MSB-first bit positions captured by any
   * ancestor mask on the path to this leaf). Each owned bit must be β-constant across all
   * keys in the leaf; {@code ownedValues[i]} provides the constant value 0/1 for owned bit
   * {@code ownedBits[i]}.
   *
   * <p>Arrays must be the same length; {@code ownedBits} must be sorted ascending. Caller
   * is responsible for verifying that the constraint actually holds; this method just
   * records the metadata. Empty arrays = no constraint (= legacy multi-entry leaf).
   */
  public void setAncestorOwnedBits(int[] ownedBits, byte[] ownedValues) {
    if (ownedBits == null || ownedBits.length == 0) {
      this.ancestorOwnedBits = EMPTY_BITS;
      this.ancestorOwnedValues = EMPTY_VALUES;
      return;
    }
    if (ownedValues == null || ownedValues.length != ownedBits.length) {
      throw new IllegalArgumentException("ownedValues length must match ownedBits length");
    }
    // Verify sorted ascending (= MSB-first absolute bit positions).
    for (int i = 1; i < ownedBits.length; i++) {
      if (ownedBits[i] <= ownedBits[i - 1]) {
        throw new IllegalArgumentException(
            "ownedBits must be sorted strictly ascending; got " + ownedBits[i - 1]
                + " followed by " + ownedBits[i]);
      }
    }
    this.ancestorOwnedBits = ownedBits.clone();
    this.ancestorOwnedValues = ownedValues.clone();
  }

  /** Returns a defensive copy of the leaf's ancestor-owned bit positions. */
  public int[] getAncestorOwnedBits() {
    return ancestorOwnedBits.length == 0 ? EMPTY_BITS : ancestorOwnedBits.clone();
  }

  /** Returns a defensive copy of the leaf's ancestor-owned bit values. */
  public byte[] getAncestorOwnedValues() {
    return ancestorOwnedValues.length == 0 ? EMPTY_VALUES : ancestorOwnedValues.clone();
  }

  /**
   * Check whether inserting {@code key} would violate the leaf's owned-bits constancy. Returns:
   * <ul>
   *   <li>-1 — key matches all owned bits (= safe to merge);
   *   <li>≥0 — the FIRST offending absolute bit position where key's value disagrees with the
   *       owned constant.
   * </ul>
   *
   * <p>HFT-grade: O(ownedBits.length), no allocation. Used by callers BEFORE merge to
   * detect β-break and trigger constancy-aware split.
   */
  public int checkOwnedBitsAgainstKey(byte[] key) {
    if (key == null || ancestorOwnedBits.length == 0) return -1;
    for (int i = 0; i < ancestorOwnedBits.length; i++) {
      final int absBit = ancestorOwnedBits[i];
      final int bytePos = absBit / 8;
      final int bitInByte = absBit % 8;
      final boolean keyBit = (bytePos < key.length)
          && ((key[bytePos] & (1 << (7 - bitInByte))) != 0);
      final boolean ownedBit = ancestorOwnedValues[i] != 0;
      if (keyBit != ownedBit) return absBit;
    }
    return -1;
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
    int offset = slotOffsets[index];
    final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    final int valueOffset = offset + 2 + suffixLen;
    final int oldValueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));

    // If new value same size or smaller, update in-place
    if (newValue.length <= oldValueLen) {
      slotMemory.set(JAVA_SHORT_UNALIGNED, valueOffset, (short) newValue.length);
      MemorySegment.copy(newValue, 0, slotMemory, ValueLayout.JAVA_BYTE, valueOffset + 2, newValue.length);
      markEntryDirty(index);
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
      // compact() relocates entries — re-read the current offset
      offset = slotOffsets[index];
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
    markEntryDirty(index);

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
    return mergeWithNodeRefsImpl(keySlice, value, valueLen);
  }

  /**
   * Phase 7b — Strict variant of {@link #mergeWithNodeRefs} that checks
   * ancestor-owned bits before merging. If the key would violate a β-constancy
   * constraint (= owned bit value disagrees with key's value), returns the
   * offending absolute bit position as a NEGATIVE int (= -(absBit + 1)). The
   * leaf is left unchanged. Caller must handle by splitting the leaf on the
   * offending β.
   *
   * <p>Returns:
   * <ul>
   *   <li>1 — merge succeeded.
   *   <li>0 — merge failed (= overflow, etc.).
   *   <li>≤ -1 — β-constancy break at absBit = (-return - 1).
   * </ul>
   *
   * <p>HFT-grade: zero allocation in the no-break path beyond what regular
   * merge does.
   */
  public int mergeWithNodeRefsStrict(byte[] key, int keyLen, byte[] value, int valueLen) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    final byte[] keySlice = keyLen == key.length ? key : Arrays.copyOf(key, keyLen);
    // β-constancy check FIRST (before any mutation).
    final int offendingBit = checkOwnedBitsAgainstKey(keySlice);
    if (offendingBit >= 0) {
      return -(offendingBit + 1);
    }
    handlePrefixForInsert(keySlice);
    return mergeWithNodeRefsImpl(keySlice, value, valueLen) ? 1 : 0;
  }

  /**
   * Insert {@code value} under {@code key}, or REPLACE the existing value if the key is
   * already present. Used by the split machinery for index types whose values are opaque
   * byte payloads (PROJECTION chunks) rather than mergeable NodeReferences bitmaps.
   *
   * <p>Same space behavior as {@link #put}/{@link #updateValue}: prefix establishment and
   * shrinkage are handled, compaction runs automatically when fragmented space suffices.
   *
   * @param key the full key
   * @param value the value payload (replaces any prior value byte-for-byte)
   * @return {@code true} if the entry was inserted or replaced; {@code false} if the page
   *         cannot fit the entry even after compaction (caller must split further)
   */
  public boolean putOrReplace(byte[] key, byte[] value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);

    handlePrefixForInsert(key);

    final int index = findEntry(key);
    if (index >= 0) {
      return updateValue(index, value);
    }
    return insertAtWithKey(-(index + 1), key, value);
  }

  private boolean mergeWithNodeRefsImpl(byte[] keySlice, byte[] value, int valueLen) {

    int index = findEntry(keySlice);

    if (index >= 0) {
      // Key exists - merge NodeReferences
      final byte[] existingValue = getValue(index);

      if (NodeReferencesSerializer.isTombstone(existingValue, 0, existingValue.length)) {
        final byte[] valueSlice = valueLen == value.length ? value : Arrays.copyOf(value, valueLen);
        return updateValue(index, valueSlice);
      }

      // HFT fast path: a single-bit packed merge into a packed bucket (the dominant churn case)
      // avoids 2 Roaring64Bitmap + 2 NodeReferences allocations. byte-identical to the slow path.
      final byte[] fastMerged =
          NodeReferencesSerializer.mergePackedSingleBit(existingValue, value, 0, valueLen);
      if (fastMerged == existingValue) {
        return true; // new key already present — merged set unchanged, slot rewrite unnecessary
      }
      if (fastMerged != null) {
        return updateValue(index, fastMerged);
      }

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

    // Bulk copy off-heap data — mutations happen in-place at sub-byte granularity, so the
    // shadow leaf needs an independent slot heap. The dirty bitmap (cleared below) tracks
    // which entry indices the writer subsequently mutates; serialize-time logic (Phase 4)
    // can emit only those entries plus rely on completePageRef for the rest.
    MemorySegment.copy(slotMemory, 0, newSlotMemory, 0, usedSlotMemorySize);

    // Deep copy on-heap arrays
    final int[] newSlotOffsets = Arrays.copyOf(slotOffsets, slotOffsets.length);

    // Deep copy prefix
    final byte[] newPrefix = commonPrefixLen > 0 ? Arrays.copyOf(commonPrefix, commonPrefixLen) : EMPTY_PREFIX;

    // Create new page with copied data
    final HOTLeafPage copy = new HOTLeafPage(recordPageKey, revision, indexType, newSlotMemory, newReleaser,
        newSlotOffsets, entryCount, usedSlotMemorySize, newPrefix, commonPrefixLen);

    // Deep copy page references (projection segment refs). Copying the PageReference itself —
    // not sharing the instance — keeps CoW discipline: a commit through one page copy mutates
    // (setPage(null)/setKey) only that copy's reference, never a historical page's view. A
    // reference already resolved to a disk key carries the key through the copy constructor, so
    // unchanged segments stay shared across revisions by reference.
    for (var entry : pageReferences.long2ObjectEntrySet()) {
      copy.pageReferences.put(entry.getLongKey(), new PageReference(entry.getValue()));
    }

    // Slot-granular CoW: the copy starts with a clean dirty bitmap (constructor already zeroed
    // it, but be explicit) and remembers `this` as its source for lazy fill at serialize time.
    copy.clearDirtyBitmap();
    copy.completePageRef = this;

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

      if (!target.put(key, value)) {
        // Abort the split whether this was the first entry or a mid-loop failure. The old
        // tolerate-mid-loop behavior truncated the source afterwards, silently dropping the
        // failed entry from BOTH halves — an undetected keyspace hole. The source is untouched
        // until truncation below, so resetting the target and returning null is a clean abort
        // (mirrors the splitToWithInsert failure path).
        target.entryCount = 0;
        target.usedSlotMemorySize = 0;
        target.commonPrefix = EMPTY_PREFIX;
        target.commonPrefixLen = 0;
        target.clearDirtyBitmap();
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

    // Truncate dirtyBitmap symmetrically — bits at indices >= splitPoint are no longer
    // reachable by any iterator. Clearing them keeps hasDirty()/iterateDirtyEntries() honest.
    truncateDirtyBitmap(splitPoint);

    // Recompute prefix for the remaining left half (may have a longer prefix now)
    recomputePrefix();

    // Invalidate PEXT
    pextValid = false;

    moveSegmentRefsAfterSplit(target);

    return splitKey;
  }

  /**
   * Route segment-reference side-map entries to {@code target} after a split moved slots
   * there. A side-map key encodes its owning slot as {@code (slotLong << 8) | segmentId},
   * where the owning slot's stored key bytes are {@code PathKeySerializer.serialize(slotLong)}
   * (the projection descriptor-slot key encoding — see
   * docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3). A reference must live on the page that
   * holds its owning slot, or readers navigating to the post-split leaf would find the
   * descriptor but not the segment. Routing by owner-slot residency (not by key-range
   * comparison) stays correct for the disc-bit split variants, whose partition is not
   * contiguous in key order. Called by every split variant after entry transfer.
   */
  private void moveSegmentRefsAfterSplit(final HOTLeafPage target) {
    if (pageReferences.isEmpty()) {
      return;
    }
    final byte[] ownerKey = new byte[8];
    final var iterator = pageReferences.long2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      final var entry = iterator.next();
      final long ownerSlot = segmentRefOwnerSlot(entry.getLongKey());
      PathKeySerializer.INSTANCE.serialize(ownerSlot, ownerKey, 0);
      if (target.findEntry(ownerKey) >= 0) {
        target.pageReferences.put(entry.getLongKey(), entry.getValue());
        iterator.remove();
      }
    }
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
    return splitToWithInsert(target, key, keyLen, value, valueLen, null);
  }

  /**
   * Same as {@link #splitToWithInsert(HOTLeafPage, byte[], int, byte[], int)} but writes
   * which half the new key landed in to {@code newSideOut[0]} on success: {@code 0}
   * (LEFT, β=0 — key stayed in {@code this}) or {@code 1} (RIGHT, β=1 — key went to
   * {@code target}). Untouched on failure.
   *
   * <p>Phase 4b-vb consumers (the C++-faithful integration path) need this so they can
   * compute {@code valueToInsert} / {@code valueToReplace} per
   * {@code integrateBiNodeIntoTree}'s semantics — pass the half WITHOUT the new key as
   * {@code valueToReplace} (replaces splitChild's slot in-place) and the half WITH the
   * new key as {@code valueToInsert} (the single new entry added at the parent level).
   *
   * @param newSideOut optional length-1 array that receives 0 or 1 on success;
   *                   {@code null} if caller does not need this info
   */
  public boolean splitToWithInsert(HOTLeafPage target, byte[] key, int keyLen,
      byte[] value, int valueLen, int @Nullable [] newSideOut) {
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

    // Save source state before truncation. The insert step below may legitimately
    // mutate slotMemory and slotOffsets before reporting failure (prefix shrink via
    // handlePrefixForInsert rebuilds every entry; insertAtSuffix/updateValue may
    // compact()), so a valid rollback must snapshot the full mutable state — not just
    // the scalar fields. Splits are O(log N)-frequency events; the extra
    // usedSlotMemorySize-byte copy is irrelevant next to the right-half transfer above.
    final int savedEntryCount = entryCount;
    final int savedUsedMemory = usedSlotMemorySize;
    final byte[] savedPrefix = commonPrefix;
    final int savedPrefixLen = commonPrefixLen;
    final long[] savedDirtyBitmap = new long[DIRTY_BITMAP_WORDS];
    snapshotDirtyBitmap(savedDirtyBitmap);
    final int[] savedSlotOffsets = Arrays.copyOf(slotOffsets, savedEntryCount);
    final byte[] savedSlotMemory = new byte[savedUsedMemory];
    MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, 0, savedSlotMemory, 0, savedUsedMemory);

    // Truncate self to left half. Do NOT call recomputePrefix() yet — it may rewrite
    // slotMemory/slotOffsets if the prefix grows; the snapshot above covers rollback,
    // but the old (shorter) prefix is still a valid prefix of all remaining keys, so
    // the insert step will simply produce correct but slightly longer suffixes.
    entryCount = splitPoint;
    recalculateUsedMemory();
    truncateDirtyBitmap(splitPoint);

    // Insert/update the key in the correct half based on disc bit.
    //
    // Value semantics depend on the index type: CAS/PATH/NAME values are
    // NodeReferences bitmaps and an existing key must be MERGED (bitmap OR).
    // PROJECTION values are opaque chunk payloads — "insert" of an existing key
    // is a REPLACE; feeding chunk bytes through the NodeReferences merge would
    // deserialize random payload bytes as roaring bitmaps and corrupt the slot.
    final boolean newKeyToRight = DiscriminativeBitComputer.isBitSet(keySlice, msdb);
    final HOTLeafPage half = newKeyToRight ? target : this;
    final boolean insertOk;
    if (indexType == IndexType.PROJECTION) {
      insertOk = half.putOrReplace(keySlice, valueSlice);
    } else {
      insertOk = half.mergeWithNodeRefs(keySlice, keySlice.length, valueSlice, valueSlice.length);
    }

    if (!insertOk || entryCount == 0 || target.entryCount == 0) {
      // Restore source page from the full snapshot — the failed insert step may have
      // compacted or prefix-rebuilt the left half before failing.
      entryCount = savedEntryCount;
      usedSlotMemorySize = savedUsedMemory;
      commonPrefix = savedPrefix;
      commonPrefixLen = savedPrefixLen;
      System.arraycopy(savedSlotOffsets, 0, slotOffsets, 0, savedEntryCount);
      MemorySegment.copy(savedSlotMemory, 0, slotMemory, ValueLayout.JAVA_BYTE, 0, savedUsedMemory);
      restoreDirtyBitmap(savedDirtyBitmap);
      pextValid = false;
      // Clear target
      target.entryCount = 0;
      target.usedSlotMemorySize = 0;
      target.commonPrefix = EMPTY_PREFIX;
      target.commonPrefixLen = 0;
      target.clearDirtyBitmap();
      return false;
    }

    markAllEntriesDirty();
    completeDump = true;

    recomputePrefix();
    pextValid = false;
    propagateOwnedBitsAfterSplit(target, msdb);
    moveSegmentRefsAfterSplit(target);
    if (newSideOut != null && newSideOut.length > 0) {
      newSideOut[0] = newKeyToRight ? 1 : 0;
    }
    return true;
  }

  /**
   * Phase 7d — On successful splitToWithInsert, propagate parent's ancestor-owned bits to
   * BOTH halves and add the split bit (msdb) as a new owned bit with the appropriate
   * constant value for each half. `this` becomes the LEFT half (β=0); {@code target} is
   * the RIGHT half (β=1).
   */
  private void propagateOwnedBitsAfterSplit(HOTLeafPage target, int msdb) {
    final int[] parentBits = this.ancestorOwnedBits;
    final byte[] parentValues = this.ancestorOwnedValues;
    final int parentLen = parentBits.length;
    // Find insertion point for msdb (sorted ascending).
    int insertPos = parentLen;
    for (int i = 0; i < parentLen; i++) {
      if (parentBits[i] > msdb) { insertPos = i; break; }
      if (parentBits[i] == msdb) { return; } // already present
    }
    final int newLen = parentLen + 1;
    final int[] newBits = new int[newLen];
    final byte[] newLeftValues = new byte[newLen];
    final byte[] newRightValues = new byte[newLen];
    if (insertPos > 0) {
      System.arraycopy(parentBits, 0, newBits, 0, insertPos);
      System.arraycopy(parentValues, 0, newLeftValues, 0, insertPos);
      System.arraycopy(parentValues, 0, newRightValues, 0, insertPos);
    }
    newBits[insertPos] = msdb;
    newLeftValues[insertPos] = 0;
    newRightValues[insertPos] = 1;
    if (insertPos < parentLen) {
      System.arraycopy(parentBits, insertPos, newBits, insertPos + 1, parentLen - insertPos);
      System.arraycopy(parentValues, insertPos, newLeftValues, insertPos + 1, parentLen - insertPos);
      System.arraycopy(parentValues, insertPos, newRightValues, insertPos + 1, parentLen - insertPos);
    }
    this.setAncestorOwnedBits(newBits, newLeftValues);
    target.setAncestorOwnedBits(newBits, newRightValues);
  }

  /**
   * Split+insert variant that splits on a caller-supplied bit instead of the leaf's
   * local MSDB. Phase-2-and-beyond infrastructure for the strict-Binna conformance
   * plan: future writer-level logic chooses {@code explicitSplitBit} based on
   * parent/sibling state (e.g., Phase 3 lazy retroactive sibling rebalance), then
   * invokes this method to apply the split.
   *
   * <p><b>Important</b>: the caller is responsible for ensuring the chosen bit
   * preserves contiguous partition (= bit equals MSDB position). Splitting on a
   * less-significant non-constant bit yields non-contiguous partition, which breaks
   * the parent's children-sorted-by-firstkey invariant. The writer should use
   * {@link #computeMsdbWithOptionalNewKey} to pre-compute MSDB and only pass MSDB
   * itself (or refrain from calling this method).
   *
   * <p>The partition algorithm is general (handles non-contiguous), but the post-split
   * structure is sensible only when the partition IS contiguous.
   *
   * <p><b>Edge cases</b>:
   * <ul>
   *   <li>{@code explicitSplitBit < 0} → {@code -1} (caller error / sentinel).</li>
   *   <li>Degenerate split (all keys on one side at this bit): {@code -1} so caller
   *       falls back to standard {@link #splitToWithInsert}.</li>
   * </ul>
   *
   * <p>HFT-grade: zero allocation beyond the necessary key/value byte arrays for transfer.
   * Bit-set checks are primitive byte loads + bit masks.
   *
   * @param target empty target page to receive the right half
   * @param key the new key to insert
   * @param keyLen the key length
   * @param value the new value to insert
   * @param valueLen the value length
   * @param explicitSplitBit absolute MSB-first bit position to use as the split bit.
   *        Must be a bit at which {@code leaf.keys + key} is non-constant (otherwise
   *        the partition is degenerate and this method returns {@code -1}). Caller is
   *        responsible for choosing a parent-friendly bit (= constant in non-split
   *        siblings, not in parent's mask, in parent's window).
   * @return the absolute split bit position (≥ 0) on success; {@code -1} on degenerate or
   *         all-identical keys. The caller must use this bit (NOT the bit derived by
   *         {@code computeDifferingBit(leftMax, rightMin)}) as the BiNode's disc bit
   *         because the partition is non-contiguous when {@code explicitSplitBit} is less
   *         significant than the leaf's MSDB.
   */
  public int splitToWithInsertOnBit(HOTLeafPage target, byte[] key, int keyLen,
      byte[] value, int valueLen, int explicitSplitBit) {
    Objects.requireNonNull(target);
    if (explicitSplitBit < 0) {
      return -1;
    }

    final int count = entryCount;
    if (count < 1) {
      return -1;
    }

    // Slice key/value to actual length
    final byte[] keySlice = keyLen == key.length ? key : Arrays.copyOf(key, keyLen);
    final byte[] valueSlice = valueLen == value.length ? value : Arrays.copyOf(value, valueLen);

    final int searchResult = findEntry(keySlice);
    final boolean isNew = searchResult < 0;

    final int splitBit = explicitSplitBit;

    // Partition existing keys by splitBit. Note: when splitBit == MSDB the partition is
    // a contiguous prefix range (left half = sorted prefix). When splitBit < MSDB
    // (less significant ancestor bit), partition may be non-contiguous: we must scan
    // and copy each key explicitly. We collect the indices for both halves and process
    // them in order.
    final int[] leftIndices = new int[count];
    final int[] rightIndices = new int[count];
    int leftN = 0;
    int rightN = 0;
    for (int i = 0; i < count; i++) {
      if (DiscriminativeBitComputer.isBitSet(getKeySlice(i), splitBit)) {
        rightIndices[rightN++] = i;
      } else {
        leftIndices[leftN++] = i;
      }
    }

    // Determine new-key side
    final boolean newKeyOnRight = DiscriminativeBitComputer.isBitSet(keySlice, splitBit);

    // Degenerate split guard: at least one existing key on each side, OR new key alone
    // makes one side. Check that final halves are non-empty.
    final int finalLeft = leftN + (isNew && !newKeyOnRight ? 1 : 0);
    final int finalRight = rightN + (isNew && newKeyOnRight ? 1 : 0);
    if (finalLeft == 0 || finalRight == 0) {
      return -1; // degenerate — caller falls back to MSDB-only split
    }

    // Snapshot full keys + values BEFORE we overwrite slotMemory (re-population on left
    // truncates entryCount → reuses slotMemory; the previously-stored entries become
    // unreachable, so we must materialize them now while still readable).
    final byte[][] allKeys = new byte[count][];
    final byte[][] allValues = new byte[count][];
    for (int i = 0; i < count; i++) {
      allKeys[i] = getKey(i);
      allValues[i] = getValue(i);
    }

    // Step 1: Insert right-half existing keys (and possibly new key) into target via put().
    // Target is empty, so put() establishes its own prefix.
    for (int i = 0; i < rightN; i++) {
      final int idx = rightIndices[i];
      if (!target.put(allKeys[idx], allValues[idx])) {
        // Allocation failed in target — clear target and return -1.
        target.entryCount = 0;
        target.usedSlotMemorySize = 0;
        target.commonPrefix = EMPTY_PREFIX;
        target.commonPrefixLen = 0;
        target.clearDirtyBitmap();
        return -1;
      }
    }
    if (newKeyOnRight) {
      if (!target.put(keySlice, valueSlice)) {
        target.entryCount = 0;
        target.usedSlotMemorySize = 0;
        target.commonPrefix = EMPTY_PREFIX;
        target.commonPrefixLen = 0;
        target.clearDirtyBitmap();
        return -1;
      }
    }

    // Step 2: Reset this leaf and re-populate from left-half indices. We've already
    // snapshotted all key/value bytes, so re-insertion via put() into the cleared
    // slotMemory is safe.
    entryCount = 0;
    usedSlotMemorySize = 0;
    commonPrefix = EMPTY_PREFIX;
    commonPrefixLen = 0;
    clearDirtyBitmap();
    pextValid = false;

    for (int i = 0; i < leftN; i++) {
      final int idx = leftIndices[i];
      if (!put(allKeys[idx], allValues[idx])) {
        // Source page exhausted slotMemory unexpectedly. We've already lost the
        // original layout (slotOffsets are overwritten); the only sane recovery
        // is to keep going if possible. Return -1 so caller treats this as
        // a failed split. The page is in an undefined intermediate state.
        return -1;
      }
    }
    if (!newKeyOnRight) {
      if (!put(keySlice, valueSlice)) {
        return -1;
      }
    }

    if (entryCount == 0 || target.entryCount == 0) {
      return -1;
    }

    markAllEntriesDirty();
    completeDump = true;

    recomputePrefix();
    target.recomputePrefix();
    pextValid = false;
    propagateOwnedBitsAfterSplit(target, splitBit);
    moveSegmentRefsAfterSplit(target);
    return splitBit;
  }

  /**
   * Pure no-insert split: partition this leaf's existing entries by an absolute MSB-first
   * bit position. Right-half entries (bit value = 1) move to {@code target}; left-half
   * entries (bit value = 0) remain in {@code this}.
   *
   * <p>Used by Phase 3 lazy retroactive sibling rebalance — when an ancestor adds a new
   * disc bit β that's non-constant in some sibling's subtree, the writer walks down to
   * each leaf in that subtree and calls this method with β to enforce per-leaf β-constancy.
   * No new key is inserted; the leaf is purely partitioned.
   *
   * <p><b>Edge cases</b>:
   * <ul>
   *   <li>{@code splitBit < 0}: returns {@code false}.</li>
   *   <li>Degenerate (all keys agree on this bit): returns {@code false} so caller knows
   *       no split was needed.</li>
   *   <li>Empty leaf: returns {@code false}.</li>
   * </ul>
   *
   * <p>HFT-grade: snapshot of all existing keys+values is unavoidable (re-population
   * truncates {@code slotMemory}). All other state lives on the call stack. Two index
   * arrays + the byte-array snapshot total ~2N pointers + N(key+value) bytes, no boxing.
   *
   * @param target empty target page to receive the right-half (β=1) entries
   * @param splitBit absolute MSB-first bit position to partition on
   * @return {@code true} if the partition was non-degenerate and applied; {@code false}
   *         if the leaf was empty / the bit was constant / target.put() failed
   */
  public boolean splitToOnBit(HOTLeafPage target, int splitBit) {
    Objects.requireNonNull(target);
    if (splitBit < 0) {
      return false;
    }
    final int count = entryCount;
    if (count < 1) {
      return false;
    }

    // Partition existing keys by splitBit.
    final int[] leftIndices = new int[count];
    final int[] rightIndices = new int[count];
    int leftN = 0;
    int rightN = 0;
    for (int i = 0; i < count; i++) {
      if (DiscriminativeBitComputer.isBitSet(getKeySlice(i), splitBit)) {
        rightIndices[rightN++] = i;
      } else {
        leftIndices[leftN++] = i;
      }
    }

    // Degenerate split guard: caller's bit-constancy check should have ruled this out,
    // but be defensive.
    if (leftN == 0 || rightN == 0) {
      return false;
    }

    // Snapshot full keys + values BEFORE we overwrite slotMemory.
    final byte[][] allKeys = new byte[count][];
    final byte[][] allValues = new byte[count][];
    for (int i = 0; i < count; i++) {
      allKeys[i] = getKey(i);
      allValues[i] = getValue(i);
    }

    // Step 1: Insert right-half entries into target (target is empty).
    for (int i = 0; i < rightN; i++) {
      final int idx = rightIndices[i];
      if (!target.put(allKeys[idx], allValues[idx])) {
        target.entryCount = 0;
        target.usedSlotMemorySize = 0;
        target.commonPrefix = EMPTY_PREFIX;
        target.commonPrefixLen = 0;
        target.clearDirtyBitmap();
        return false;
      }
    }

    // Step 2: Reset this leaf and re-populate from left-half indices.
    entryCount = 0;
    usedSlotMemorySize = 0;
    commonPrefix = EMPTY_PREFIX;
    commonPrefixLen = 0;
    clearDirtyBitmap();
    pextValid = false;

    for (int i = 0; i < leftN; i++) {
      final int idx = leftIndices[i];
      if (!put(allKeys[idx], allValues[idx])) {
        return false;
      }
    }

    if (entryCount == 0 || target.entryCount == 0) {
      return false;
    }

    markAllEntriesDirty();
    completeDump = true;

    recomputePrefix();
    target.recomputePrefix();
    pextValid = false;
    moveSegmentRefsAfterSplit(target);
    return true;
  }

  /**
   * Returns true iff this leaf's existing entries span both bit values at the given
   * absolute MSB-first bit position. Used by Phase 3 lazy rebalance for cheap
   * per-leaf β-constancy checks.
   *
   * <p>HFT-grade: zero allocation, primitive byte loads + bit masks. Short-circuits as
   * soon as both 0 and 1 are observed.
   */
  public boolean isBitNonConstantInLeaf(int absBit) {
    final int n = entryCount;
    if (n < 2) {
      return false;
    }
    final boolean firstBit = DiscriminativeBitComputer.isBitSet(getKeySlice(0), absBit);
    boolean sawZero = !firstBit;
    boolean sawOne = firstBit;
    for (int i = 1; i < n; i++) {
      final boolean v = DiscriminativeBitComputer.isBitSet(getKeySlice(i), absBit);
      if (v) sawOne = true;
      else sawZero = true;
      if (sawZero && sawOne) return true;
    }
    return false;
  }

  /**
   * Returns true iff the existing leaf entries plus the new key span both bit values at
   * absolute MSB-first bit position {@code absBit}. Public so writer code can scan
   * candidate split bits without re-deriving leaf-private state.
   *
   * <p>HFT-grade: zero allocation, primitive byte loads + bit masks via
   * {@link DiscriminativeBitComputer#isBitSet}.
   */
  public boolean isBitNonConstantInLeafPlusNewKey(int absBit, byte[] newKey, boolean isNew) {
    final int n = entryCount;
    if (n == 0) {
      // Only the new key — vacuously constant.
      return false;
    }
    final boolean firstBit = DiscriminativeBitComputer.isBitSet(getKeySlice(0), absBit);
    boolean sawZero = !firstBit;
    boolean sawOne = firstBit;
    for (int i = 1; i < n; i++) {
      final boolean v = DiscriminativeBitComputer.isBitSet(getKeySlice(i), absBit);
      if (v) sawOne = true;
      else sawZero = true;
      if (sawZero && sawOne) return true;
    }
    if (isNew) {
      final boolean v = DiscriminativeBitComputer.isBitSet(newKey, absBit);
      if (v) sawOne = true;
      else sawZero = true;
    }
    return sawZero && sawOne;
  }

  /**
   * Compute the MSDB (most significant disc bit) across the current leaf entries plus
   * an optional new key. Public so writer code can pre-compute MSDB and test alternative
   * split bits before invoking {@link #splitToWithInsertOnBit}.
   *
   * <p>If {@code newKey} is {@code null} or the leaf already contains it, the MSDB is
   * computed across existing entries only. Otherwise the new key is virtually inserted
   * at its sorted position and MSDB is computed across the augmented sequence.
   *
   * @param newKey optional new key being virtually inserted (may be {@code null})
   * @return absolute MSB-first bit position of MSDB, or {@code -1} if all keys are
   *         identical (no discriminative bit exists)
   */
  public int computeMsdbWithOptionalNewKey(byte @Nullable [] newKey) {
    if (newKey == null) {
      return findMsdbBit();
    }
    final int searchResult = findEntry(newKey);
    if (searchResult >= 0) {
      // newKey already present — same as no-new-key case
      return findMsdbBit();
    }
    final int insertPos = -(searchResult + 1);
    return findMsdbWithNewKey(newKey, insertPos);
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
   * Phase 2 helper: compute the MSDB this leaf would have if {@code key} were inserted.
   * Returns {@code -1} if all keys (including {@code key}) would be identical.
   *
   * <p>Used by {@code HOTTrieWriter.findOffendingAncestorBit} to detect when an insert
   * would introduce a new MSDB that coincides with an ancestor disc bit β — the safe-to-
   * eager-split case (contiguous partition on β).
   */
  public int computeMsdbWithKey(byte[] key) {
    if (entryCount == 0) return -1;
    final int searchResult = findEntry(key);
    final boolean isNew = searchResult < 0;
    final int insertPos = isNew ? -(searchResult + 1) : searchResult;
    return isNew ? findMsdbWithNewKey(key, insertPos) : findMsdbBit();
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
   * Public wrapper around {@link #recomputePrefix()} for cross-package callers (versioning combine).
   * Intentionally narrow — exposes only what {@link io.sirix.settings.VersioningType} needs.
   */
  public void recomputePrefixForCombine() {
    recomputePrefix();
  }

  /**
   * @return total raw byte size of the entries marked dirty in {@link #dirtyBitmap}, computed as
   *         the sum of {@code 2 + suffixLen + 2 + valueLen} per dirty entry.
   */
  public int getDirtyEntriesUsedSize() {
    int total = 0;
    for (int w = 0; w < DIRTY_BITMAP_WORDS; w++) {
      long word = dirtyBitmap[w];
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int idx = (w << 6) | bit;
        if (idx < entryCount) {
          final int off = slotOffsets[idx];
          final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, off));
          final int valueLen =
              Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, off + 2 + suffixLen));
          total += 2 + suffixLen + 2 + valueLen;
        }
        word &= word - 1L;
      }
    }
    return total;
  }

  /**
   * Count the dirty entries (within {@code entryCount}) — exposed for the sparse-emit serializer.
   */
  public int getDirtyEntryCount() {
    int count = 0;
    for (int w = 0; w < DIRTY_BITMAP_WORDS; w++) {
      long word = dirtyBitmap[w];
      // Mask off bits beyond entryCount in the highest word that overlaps it.
      final int wordEntryStart = w << 6;
      if (wordEntryStart >= entryCount) {
        break;
      }
      final int wordEntryEnd = wordEntryStart + 64;
      if (wordEntryEnd > entryCount) {
        final int rem = entryCount - wordEntryStart;
        word &= rem == 0 ? 0L : ((1L << rem) - 1L);
      }
      count += Long.bitCount(word);
    }
    return count;
  }

  /**
   * Pack the dirty entries' raw bytes into {@code dst}, starting at offset 0, and write each
   * entry's new packed offset into {@code newOffsets} in walk order. Returns the total bytes
   * written.
   *
   * <p>Caller must size {@code dst} &gt;= {@link #getDirtyEntriesUsedSize()} and
   * {@code newOffsets} &gt;= {@link #getDirtyEntryCount()}.</p>
   *
   * @param dst        destination byte array (typically a sink-bound scratch)
   * @param newOffsets per-entry packed offset (for the wire format's slotOffsets table)
   * @return total bytes packed into {@code dst}
   */
  public int packDirtyEntries(final byte[] dst, final int[] newOffsets) {
    int writeOff = 0;
    int outIdx = 0;
    for (int w = 0; w < DIRTY_BITMAP_WORDS; w++) {
      long word = dirtyBitmap[w];
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int idx = (w << 6) | bit;
        if (idx < entryCount) {
          final int srcOff = slotOffsets[idx];
          final int suffixLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, srcOff));
          final int valueLen =
              Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, srcOff + 2 + suffixLen));
          final int entrySize = 2 + suffixLen + 2 + valueLen;
          MemorySegment.copy(slotMemory, ValueLayout.JAVA_BYTE, srcOff, dst, writeOff, entrySize);
          newOffsets[outIdx++] = writeOff;
          writeOff += entrySize;
        }
        word &= word - 1L;
      }
    }
    return writeOff;
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
   * Mark every entry in {@code [0, entryCount)} as dirty. Used when a full-dump emit is needed
   * (e.g., FULL strategy or window-edge revision under SLIDING_SNAPSHOT) — the serialize path
   * uses {@link #hasDirty()} / {@link #iterateDirtyEntries(IntConsumer)} to decide what to write,
   * so flipping every bit on guarantees a full-leaf fragment.
   */
  public void markAllEntriesDirty() {
    final int full = entryCount >>> 6;
    for (int w = 0; w < full; w++) {
      dirtyBitmap[w] = -1L;
    }
    final int rem = entryCount & 63;
    if (rem != 0 && full < DIRTY_BITMAP_WORDS) {
      dirtyBitmap[full] |= (1L << rem) - 1L;
    }
  }

  /**
   * Ensure this leaf carries every entry needed for full-fragment serialization under {@code type}.
   * Since {@link #copy()} bulk-copies the source slot heap, the modifying leaf already physically
   * contains every entry from its {@link #completePageRef} — this hook only needs to mark all
   * entries dirty so the sparse-emit path includes them. If {@code completePageRef} is {@code null}
   * (fresh leaf), the writer-level mutators have already marked each insert dirty, so this is a
   * no-op for the dirty bitmap; callers under FULL still get a full emit because every insert is
   * naturally dirty on a fresh leaf.
   *
   * <p>Centralised here so the serializer doesn't reach into private fields.</p>
   *
   * @param type the active versioning strategy
   */
  public void materializeFromCompletePageRef(final io.sirix.settings.VersioningType type) {
    Objects.requireNonNull(type);
    if (type == io.sirix.settings.VersioningType.FULL) {
      markAllEntriesDirty();
    }
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
    // Double-check after increment to prevent acquire-after-close/orphan race.
    if (closed.get() || isOrphaned) {
      // Lost the race with close()/markOrphaned(): undo the guard. If this was the last
      // guard on an orphaned-but-not-yet-closed page, complete the deferred teardown so
      // the off-heap slot is not leaked.
      if (guardCount.decrementAndGet() == 0 && isOrphaned) {
        if (closed.compareAndSet(false, true)) {
          releaseMemory();
        }
      }
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
   * Close the page and release its off-heap memory. Thread-safe; only the first effective
   * call releases the memory segment.
   *
   * <p><b>Guard-aware teardown.</b> A page with live guards is in active use by a reader, so
   * the slot release is deferred: the page is marked orphaned and the last {@link #releaseGuard()}
   * performs the actual teardown. This guarantees eviction can never free a slot out from
   * under a reader that has already acquired a guard — the reader-side eviction race that
   * surfaced as silent key loss under memory pressure. A racing {@link #acquireGuard()} sees
   * {@link #isOrphaned} on its post-increment re-check and retries instead.
   */
  public void close() {
    isOrphaned = true;
    if (guardCount.get() == 0) {
      if (closed.compareAndSet(false, true)) {
        releaseMemory();
      }
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

  /**
   * Remove a side-map reference (a projection segment that no longer exists after a leaf
   * shrank or was tombstoned). Returns the removed reference or {@code null} if absent.
   */
  public @Nullable PageReference removePageReference(long key) {
    return pageReferences.remove(key);
  }

  /** Number of side-map references on this page. */
  public int segmentRefCount() {
    return pageReferences.size();
  }

  /**
   * Side-map keys in ascending order — the serializer emits entries sorted so identical maps
   * produce identical bytes.
   */
  public long[] segmentRefKeysSorted() {
    final long[] keys = pageReferences.keySet().toLongArray();
    Arrays.sort(keys);
    return keys;
  }

  /**
   * Commit descent into side-map references, mirroring {@code KeyValueLeafPage#commit} for
   * overflow references (#1076): segment pages hang off this map WITHOUT a
   * TransactionIntentLog entry (logKey stays NULL), so the default {@code Page#commit}'s
   * logKey filter would skip them and the leaf would serialize dangling {@code -1} keys. The
   * storage-engine writer's commit branch writes each in-memory
   * {@link ProjectionSegmentPage} and assigns its durable offset key strictly before this
   * leaf's own bytes are produced.
   */
  @Override
  public void commit(final StorageEngineWriter pageWriteTrx) {
    if (pageReferences.isEmpty()) {
      return;
    }
    for (final PageReference reference : pageReferences.values()) {
      if (!(reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
          && reference.getLogKey() == Constants.NULL_ID_INT)) {
        pageWriteTrx.commit(reference);
      }
    }
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

  @Override
  public long getActualMemorySize() {
    return slotMemory != null ? slotMemory.byteSize() : 0;
  }

  @Override
  public void markAccessed() {
    hot = true;
  }

  @Override
  public boolean isHot() {
    return hot;
  }

  @Override
  public void clearHot() {
    hot = false;
  }

  @Override
  public void incrementVersion() {
    version.incrementAndGet();
  }

  /**
   * Get current version.
   *
   * @return version number
   */
  public int getVersion() {
    return version.get();
  }

  // ===== Slot-granular CoW: dirty bitmap accessors =====

  /**
   * Mark an entry index as dirty. Centralized in mutators so writers stay oblivious.
   * No bounds-check — callers already validated against entryCount before invoking a mutator.
   */
  void markEntryDirty(final int index) {
    dirtyBitmap[index >>> 6] |= 1L << (index & 63);
  }

  /**
   * Check if an entry index has been mutated since copy().
   */
  public boolean isEntryDirty(final int index) {
    return (dirtyBitmap[index >>> 6] & (1L << (index & 63))) != 0;
  }

  /**
   * Clear every bit in the dirty bitmap. Called on copy() so the new leaf starts clean. Public so
   * cross-package callers (versioning combine) can reset dirty marks on a freshly-merged leaf.
   */
  public void clearDirtyBitmap() {
    for (int i = 0; i < DIRTY_BITMAP_WORDS; i++) {
      dirtyBitmap[i] = 0L;
    }
  }

  /**
   * Number of entries currently marked dirty in the bitmap. Used by tests pinning the
   * slot-granular sparse-fragment invariants — a single-key write must leave dirtyEntryCount
   * equal to 1 so the rev's on-disk fragment carries only that one slot.
   *
   * @return total population count across the dirty bitmap, capped at {@link #entryCount}
   */
  public int dirtyEntryCount() {
    int count = 0;
    for (int i = 0; i < DIRTY_BITMAP_WORDS; i++) {
      count += Long.bitCount(dirtyBitmap[i]);
    }
    return count;
  }

  /**
   * @return {@code true} if at least one entry has been marked dirty.
   */
  public boolean hasDirty() {
    for (int i = 0; i < DIRTY_BITMAP_WORDS; i++) {
      if (dirtyBitmap[i] != 0L) {
        return true;
      }
    }
    return false;
  }

  /**
   * Read a raw word from the dirty bitmap. Exposed for serialization and tests.
   */
  public long getDirtyBitmapWord(final int wordIndex) {
    return dirtyBitmap[wordIndex];
  }

  /**
   * Set this leaf's reference to the complete (post-merge) page. The complete page supplies
   * preserved entries when a versioning strategy needs a full-fragment emit at serialize time.
   */
  public void setCompletePageRef(final @Nullable HOTLeafPage completePage) {
    this.completePageRef = completePage;
  }

  /**
   * @return the complete page reference, or {@code null} if this is a fresh / fully-materialized leaf.
   */
  public @Nullable HOTLeafPage getCompletePageRef() {
    return completePageRef;
  }

  public boolean isCompleteDump() {
    return completeDump;
  }

  public void setCompleteDump(final boolean completeDump) {
    this.completeDump = completeDump;
  }

  /**
   * Walk the dirty-bitmap word-by-word, invoking {@code consumer} on each set bit's index.
   * Zero-allocation; uses {@link Long#numberOfTrailingZeros} for branchless bit iteration.
   */
  public void iterateDirtyEntries(final IntConsumer consumer) {
    for (int w = 0; w < DIRTY_BITMAP_WORDS; w++) {
      long word = dirtyBitmap[w];
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        consumer.accept((w << 6) | bit);
        word &= word - 1L;
      }
    }
  }

  /**
   * Shift dirtyBitmap to mirror an insert at {@code pos}: bits in {@code [pos, MAX_ENTRIES-1)}
   * move up by 1. The bit at position {@code pos} after the shift is cleared; the caller
   * subsequently marks the inserted index dirty.
   *
   * <p>Semantically: dirtyBitmap behaves like a bitwise variant of
   * {@code System.arraycopy(slotOffsets, pos, slotOffsets, pos + 1, entryCount - pos)}.</p>
   */
  void shiftDirtyBitmapForInsert(final int pos) {
    if (pos < 0 || pos >= MAX_ENTRIES) {
      return;
    }
    final int posWord = pos >>> 6;
    final int posBit = pos & 63;

    // Walk MSB-word downward so we never overwrite a source word before reading it.
    for (int w = DIRTY_BITMAP_WORDS - 1; w > posWord; w--) {
      final long shifted = dirtyBitmap[w] << 1;
      // Inject bit 63 of the lower word as bit 0 of this word.
      final long carryIn = (dirtyBitmap[w - 1] >>> 63) & 1L;
      dirtyBitmap[w] = shifted | carryIn;
    }
    // posWord: keep [0, posBit), shift [posBit, 62] up to [posBit+1, 63], drop original bit 63
    // (the loop above already absorbed it as carry-in).
    final long lowMask = posBit == 0 ? 0L : (1L << posBit) - 1L;
    final long lowBits = dirtyBitmap[posWord] & lowMask;
    final long shiftedHigh = (dirtyBitmap[posWord] & ~lowMask) << 1;
    long updated = lowBits | shiftedHigh;
    // Clear the inserted slot's bit; caller marks it explicitly afterwards.
    updated &= ~(1L << posBit);
    dirtyBitmap[posWord] = updated;
  }

  /**
   * Clear dirty-bitmap bits at indices &gt;= {@code newEntryCount}. Used after a split/truncation
   * to drop stale bits that no longer correspond to a reachable entry.
   */
  void truncateDirtyBitmap(final int newEntryCount) {
    if (newEntryCount <= 0) {
      clearDirtyBitmap();
      return;
    }
    if (newEntryCount >= MAX_ENTRIES) {
      return;
    }
    final int word = newEntryCount >>> 6;
    final int bit = newEntryCount & 63;
    if (bit == 0) {
      dirtyBitmap[word] = 0L;
    } else {
      dirtyBitmap[word] &= (1L << bit) - 1L;
    }
    for (int w = word + 1; w < DIRTY_BITMAP_WORDS; w++) {
      dirtyBitmap[w] = 0L;
    }
  }

  /**
   * Snapshot the dirty-bitmap into {@code dst} (must have length &gt;= {@link #DIRTY_BITMAP_WORDS}).
   * Used by {@link #splitToWithInsert} for atomic rollback alongside entryCount/usedSlotMemorySize.
   */
  void snapshotDirtyBitmap(final long[] dst) {
    System.arraycopy(dirtyBitmap, 0, dst, 0, DIRTY_BITMAP_WORDS);
  }

  /**
   * Restore the dirty-bitmap from a previously {@link #snapshotDirtyBitmap snapshotted} buffer.
   */
  void restoreDirtyBitmap(final long[] src) {
    System.arraycopy(src, 0, dirtyBitmap, 0, DIRTY_BITMAP_WORDS);
  }

  /**
   * Shift dirtyBitmap to mirror a deletion at {@code pos}: bits in {@code (pos, MAX_ENTRIES)}
   * move down by 1, the bit at {@code pos} is dropped, and the previously-MSB bit becomes 0.
   *
   * <p>HOTLeafPage's {@link #delete(byte[])} currently tombstones in place (no slotOffsets shift);
   * this helper exists for completeness so future compacting-delete code stays correct.</p>
   */
  void shiftDirtyBitmapForDelete(final int pos) {
    if (pos < 0 || pos >= MAX_ENTRIES) {
      return;
    }
    final int posWord = pos >>> 6;
    final int posBit = pos & 63;

    // posWord: keep [0, posBit), shift (posBit, 63] down by 1 to [posBit, 62]. Bit 63 carries in
    // from posWord+1 bit 0 (if any).
    final long lowMask = posBit == 0 ? 0L : (1L << posBit) - 1L;
    final long lowBits = dirtyBitmap[posWord] & lowMask;
    final long highMask = posBit == 63 ? 0L : ~((1L << (posBit + 1)) - 1L);
    final long shiftedHigh = (dirtyBitmap[posWord] & highMask) >>> 1;
    long updated = lowBits | shiftedHigh;
    if (posWord + 1 < DIRTY_BITMAP_WORDS) {
      updated |= (dirtyBitmap[posWord + 1] & 1L) << 63;
    }
    dirtyBitmap[posWord] = updated;
    // Words above posWord: shift down 1, with carry-in from the next word's bit 0.
    for (int w = posWord + 1; w < DIRTY_BITMAP_WORDS; w++) {
      long shifted = dirtyBitmap[w] >>> 1;
      if (w + 1 < DIRTY_BITMAP_WORDS) {
        shifted |= (dirtyBitmap[w + 1] & 1L) << 63;
      }
      dirtyBitmap[w] = shifted;
    }
  }

  @Override
  public String toString() {
    return "HOTLeafPage{" + "pageKey=" + recordPageKey + ", revision=" + revision + ", indexType=" + indexType
        + ", entryCount=" + entryCount + ", usedSlotMemorySize=" + usedSlotMemorySize + ", guardCount="
        + guardCount.get() + ", closed=" + closed.get() + '}';
  }
}

