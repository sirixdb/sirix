package io.sirix.page;

import io.sirix.settings.Constants;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * LeanStore-style unified page layout for {@link KeyValueLeafPage}.
 *
 * <p>All page metadata, slot directory, and record data live in a single contiguous
 * {@link MemorySegment}. In-memory format = on-disk format. ZERO conversion at commit time.
 *
 * <h2>Page Layout</h2>
 * <pre>
 * Offset   Size      Field
 * ──────── ───────── ──────────────────────
 * 0        8         recordPageKey (long)
 * 8        4         revision (int)
 * 12       2         populatedCount (u16)
 * 14       4         heapEnd (int, relative to HEAP_START)
 * 18       4         heapUsed (int, bytes of live data)
 * 22       1         indexType (byte)
 * 23       1         flags (bit 0: areDeweyIDsStored, bit 1: hasFsstTable)
 * 24       8         reserved
 * ──────── ───────── ──────────────────────
 * 32       128       slotBitmap (16 longs = 1024 bits)
 * ──────── ───────── ──────────────────────
 * 160      8192      slotDirectory (1024 × 8 bytes)
 *                      [heapOffset:4][dataLength:3 + nodeKindId:1]
 * ──────── ───────── ──────────────────────
 * 8352     ...       heap (bump-allocated forward, compact varint records)
 * </pre>
 *
 * <h2>Slot Directory Entry (8 bytes each)</h2>
 * <pre>
 * Bytes 0-3: heapOffset (int) — offset into heap region (relative to HEAP_START)
 * Bytes 4-6: dataLength (3 bytes, unsigned) — total byte length of the record in the heap
 * Byte 7:   nodeKindId (1 byte) — NodeKind ordinal for fast dispatch
 * </pre>
 *
 * <h2>Heap Record Format</h2>
 * <pre>
 * [nodeKind: 1 byte]
 * [fieldOffsetTable: fieldCount × 1 byte] — O(1) access to any field
 * [data region: varint fields + hash + optional payload]
 * </pre>
 */
public final class PageLayout {

  private PageLayout() {
    throw new AssertionError("Utility class");
  }

  // ==================== PAGE SIZING ====================

  /** Initial page allocation size (64 KB). */
  public static final int INITIAL_PAGE_SIZE = 64 * 1024;

  /** Maximum slots per page (must match Constants.NDP_NODE_COUNT). */
  public static final int SLOT_COUNT = Constants.NDP_NODE_COUNT; // 1024

  /** Slot count exponent: 2^10 = 1024 */
  public static final int SLOT_COUNT_EXPONENT = Constants.NDP_NODE_COUNT_EXPONENT; // 10

  // ==================== HEADER LAYOUT (32 bytes) ====================

  /** Total header size in bytes. */
  public static final int HEADER_SIZE = 32;

  /** Offset of recordPageKey (long, 8 bytes). */
  public static final int OFF_RECORD_PAGE_KEY = 0;

  /** Offset of revision (int, 4 bytes). */
  public static final int OFF_REVISION = 8;

  /** Offset of populatedCount (unsigned short, 2 bytes). */
  public static final int OFF_POPULATED_COUNT = 12;

  /** Offset of heapEnd (int, 4 bytes) — relative to HEAP_START. */
  public static final int OFF_HEAP_END = 14;

  /** Offset of heapUsed (int, 4 bytes) — bytes of live (non-abandoned) data. */
  public static final int OFF_HEAP_USED = 18;

  /** Offset of indexType (byte, 1 byte). */
  public static final int OFF_INDEX_TYPE = 22;

  /** Offset of flags (byte, 1 byte). */
  public static final int OFF_FLAGS = 23;

  /** Offset of reserved region (8 bytes). */
  public static final int OFF_RESERVED = 24;

  // ==================== FLAGS ====================

  /** Flag bit 0: DeweyIDs are stored inline in records. */
  public static final int FLAG_DEWEY_IDS_STORED = 1;

  /** Flag bit 1: FSST symbol table is present. */
  public static final int FLAG_HAS_FSST_TABLE = 2;

  // ==================== BITMAP LAYOUT (128 bytes) ====================

  /** Offset where the slot bitmap starts. */
  public static final int BITMAP_OFF = HEADER_SIZE; // 32

  /** Number of 64-bit words in the bitmap. */
  public static final int BITMAP_WORDS = 16;

  /** Total size of the bitmap region in bytes. */
  public static final int BITMAP_SIZE = BITMAP_WORDS * Long.BYTES; // 128

  // ==================== SLOT DIRECTORY LAYOUT (8192 bytes) ====================

  /** Offset where the slot directory starts. */
  public static final int DIR_OFF = BITMAP_OFF + BITMAP_SIZE; // 160

  /** Size of each directory entry in bytes. */
  public static final int DIR_ENTRY_SIZE = 8;

  /** Total size of the slot directory region in bytes. */
  public static final int DIR_SIZE = SLOT_COUNT * DIR_ENTRY_SIZE; // 8192

  // ==================== HEAP LAYOUT ====================

  /** Offset where the heap region starts (bump-allocated forward). */
  public static final int HEAP_START = DIR_OFF + DIR_SIZE; // 8352

  // ==================== DIRECTORY ENTRY FIELD OFFSETS ====================

  /** Offset within a directory entry for the heap offset (int, 4 bytes). */
  private static final int DIRENTRY_OFF_HEAP_OFFSET = 0;

  /** Offset within a directory entry for dataLength (3 bytes) + nodeKindId (1 byte). */
  private static final int DIRENTRY_OFF_LENGTH_AND_KIND = 4;

  /** Sentinel value indicating an empty directory entry. */
  public static final int DIR_ENTRY_EMPTY = -1;

  // ==================== VALUE LAYOUTS ====================

  private static final ValueLayout.OfLong JAVA_LONG_UNALIGNED =
      ValueLayout.JAVA_LONG_UNALIGNED;

  private static final ValueLayout.OfInt JAVA_INT_UNALIGNED =
      ValueLayout.JAVA_INT.withByteAlignment(1);

  private static final ValueLayout.OfShort JAVA_SHORT_UNALIGNED =
      ValueLayout.JAVA_SHORT.withByteAlignment(1);

  // ==================== HEADER ACCESSORS ====================

  /** Read the recordPageKey from the page header. */
  public static long getRecordPageKey(final MemorySegment page) {
    return page.get(JAVA_LONG_UNALIGNED, OFF_RECORD_PAGE_KEY);
  }

  /** Write the recordPageKey into the page header. */
  public static void setRecordPageKey(final MemorySegment page, final long key) {
    page.set(JAVA_LONG_UNALIGNED, OFF_RECORD_PAGE_KEY, key);
  }

  /** Read the revision from the page header. */
  public static int getRevision(final MemorySegment page) {
    return page.get(JAVA_INT_UNALIGNED, OFF_REVISION);
  }

  /** Write the revision into the page header. */
  public static void setRevision(final MemorySegment page, final int revision) {
    page.set(JAVA_INT_UNALIGNED, OFF_REVISION, revision);
  }

  /** Read the populated slot count from the page header. */
  public static int getPopulatedCount(final MemorySegment page) {
    return Short.toUnsignedInt(page.get(JAVA_SHORT_UNALIGNED, OFF_POPULATED_COUNT));
  }

  /** Write the populated slot count into the page header. */
  public static void setPopulatedCount(final MemorySegment page, final int count) {
    page.set(JAVA_SHORT_UNALIGNED, OFF_POPULATED_COUNT, (short) count);
  }

  /** Read the heap end position (relative to HEAP_START) from the page header. */
  public static int getHeapEnd(final MemorySegment page) {
    return page.get(JAVA_INT_UNALIGNED, OFF_HEAP_END);
  }

  /** Write the heap end position (relative to HEAP_START) into the page header. */
  public static void setHeapEnd(final MemorySegment page, final int heapEnd) {
    page.set(JAVA_INT_UNALIGNED, OFF_HEAP_END, heapEnd);
  }

  /** Read the heap used bytes from the page header. */
  public static int getHeapUsed(final MemorySegment page) {
    return page.get(JAVA_INT_UNALIGNED, OFF_HEAP_USED);
  }

  /** Write the heap used bytes into the page header. */
  public static void setHeapUsed(final MemorySegment page, final int heapUsed) {
    page.set(JAVA_INT_UNALIGNED, OFF_HEAP_USED, heapUsed);
  }

  /** Read the index type byte from the page header. */
  public static byte getIndexType(final MemorySegment page) {
    return page.get(ValueLayout.JAVA_BYTE, OFF_INDEX_TYPE);
  }

  /** Write the index type byte into the page header. */
  public static void setIndexType(final MemorySegment page, final byte indexType) {
    page.set(ValueLayout.JAVA_BYTE, OFF_INDEX_TYPE, indexType);
  }

  /** Read the flags byte from the page header. */
  public static byte getFlags(final MemorySegment page) {
    return page.get(ValueLayout.JAVA_BYTE, OFF_FLAGS);
  }

  /** Write the flags byte into the page header. */
  public static void setFlags(final MemorySegment page, final byte flags) {
    page.set(ValueLayout.JAVA_BYTE, OFF_FLAGS, flags);
  }

  /** Check if DeweyIDs are stored (flag bit 0). */
  public static boolean areDeweyIDsStored(final MemorySegment page) {
    return (getFlags(page) & FLAG_DEWEY_IDS_STORED) != 0;
  }

  /** Check if FSST symbol table is present (flag bit 1). */
  public static boolean hasFsstTable(final MemorySegment page) {
    return (getFlags(page) & FLAG_HAS_FSST_TABLE) != 0;
  }

  // ==================== BITMAP ACCESSORS ====================

  /** Read a bitmap word (one of 16 longs). */
  public static long getBitmapWord(final MemorySegment page, final int wordIndex) {
    return page.get(JAVA_LONG_UNALIGNED, BITMAP_OFF + ((long) wordIndex << 3));
  }

  /** Write a bitmap word (one of 16 longs). */
  public static void setBitmapWord(final MemorySegment page, final int wordIndex, final long word) {
    page.set(JAVA_LONG_UNALIGNED, BITMAP_OFF + ((long) wordIndex << 3), word);
  }

  /** Check if a slot is populated in the bitmap. */
  public static boolean isSlotPopulated(final MemorySegment page, final int slotIndex) {
    final long word = getBitmapWord(page, slotIndex >>> 6);
    return (word & (1L << (slotIndex & 63))) != 0;
  }

  /** Set a slot's bit in the bitmap. */
  public static void markSlotPopulated(final MemorySegment page, final int slotIndex) {
    final int wordIndex = slotIndex >>> 6;
    final long word = getBitmapWord(page, wordIndex);
    setBitmapWord(page, wordIndex, word | (1L << (slotIndex & 63)));
  }

  /** Clear a slot's bit in the bitmap. */
  public static void clearSlotPopulated(final MemorySegment page, final int slotIndex) {
    final int wordIndex = slotIndex >>> 6;
    final long word = getBitmapWord(page, wordIndex);
    setBitmapWord(page, wordIndex, word & ~(1L << (slotIndex & 63)));
  }

  /**
   * Count populated slots using Long.bitCount across all bitmap words.
   *
   * @return number of populated slots (0 to 1024)
   */
  public static int countPopulatedSlots(final MemorySegment page) {
    int count = 0;
    for (int i = 0; i < BITMAP_WORDS; i++) {
      count += Long.bitCount(getBitmapWord(page, i));
    }
    return count;
  }

  /**
   * Copy the entire bitmap from the page into a Java long[] array.
   * Useful for snapshot/iteration without repeated segment access.
   */
  public static void copyBitmapTo(final MemorySegment page, final long[] dest) {
    for (int i = 0; i < BITMAP_WORDS; i++) {
      dest[i] = getBitmapWord(page, i);
    }
  }

  /**
   * Write the entire bitmap from a Java long[] array into the page.
   */
  public static void copyBitmapFrom(final MemorySegment page, final long[] src) {
    for (int i = 0; i < BITMAP_WORDS; i++) {
      setBitmapWord(page, i, src[i]);
    }
  }

  // ==================== SLOT DIRECTORY ACCESSORS ====================

  /**
   * Compute the absolute byte offset of a slot directory entry.
   */
  public static long dirEntryOffset(final int slotIndex) {
    return DIR_OFF + (long) slotIndex * DIR_ENTRY_SIZE;
  }

  /**
   * Read the heap offset from a slot directory entry.
   * Returns {@link #DIR_ENTRY_EMPTY} if the slot is not populated
   * (caller must check bitmap first for correctness).
   */
  public static int getDirHeapOffset(final MemorySegment page, final int slotIndex) {
    return page.get(JAVA_INT_UNALIGNED, dirEntryOffset(slotIndex) + DIRENTRY_OFF_HEAP_OFFSET);
  }

  /**
   * Read the data length from a slot directory entry.
   * The length is stored in 3 bytes (unsigned, big-endian packing within the 4-byte field).
   * Byte layout: [dataLength_b2][dataLength_b1][dataLength_b0][nodeKindId]
   *
   * @return data length in bytes (0 to 16,777,215)
   */
  public static int getDirDataLength(final MemorySegment page, final int slotIndex) {
    final long base = dirEntryOffset(slotIndex) + DIRENTRY_OFF_LENGTH_AND_KIND;
    // Read 4 bytes as int, mask off the lowest byte (nodeKindId)
    final int packed = page.get(JAVA_INT_UNALIGNED, base);
    return packed >>> 8; // top 3 bytes = data length
  }

  /**
   * Read the node kind ID from a slot directory entry (lowest byte of the 4-byte field).
   *
   * @return node kind ID (0-255)
   */
  public static int getDirNodeKindId(final MemorySegment page, final int slotIndex) {
    final long base = dirEntryOffset(slotIndex) + DIRENTRY_OFF_LENGTH_AND_KIND;
    final int packed = page.get(JAVA_INT_UNALIGNED, base);
    return packed & 0xFF;
  }

  /**
   * Write a complete slot directory entry.
   *
   * @param page       the page MemorySegment
   * @param slotIndex  the slot index (0 to SLOT_COUNT-1)
   * @param heapOffset offset into the heap region (relative to HEAP_START)
   * @param dataLength total byte length of the record in the heap (0 to 16,777,215)
   * @param nodeKindId NodeKind ordinal (0 to 255)
   */
  public static void setDirEntry(final MemorySegment page, final int slotIndex,
      final int heapOffset, final int dataLength, final int nodeKindId) {
    final long base = dirEntryOffset(slotIndex);
    page.set(JAVA_INT_UNALIGNED, base + DIRENTRY_OFF_HEAP_OFFSET, heapOffset);
    // Pack: top 3 bytes = dataLength, bottom byte = nodeKindId
    final int packed = (dataLength << 8) | (nodeKindId & 0xFF);
    page.set(JAVA_INT_UNALIGNED, base + DIRENTRY_OFF_LENGTH_AND_KIND, packed);
  }

  /**
   * Clear a slot directory entry (set to all zeros).
   */
  public static void clearDirEntry(final MemorySegment page, final int slotIndex) {
    final long base = dirEntryOffset(slotIndex);
    page.set(JAVA_LONG_UNALIGNED, base, 0L);
  }

  // ==================== HEAP MANAGEMENT ====================

  /**
   * Compute the absolute byte offset for a heap-relative offset.
   *
   * @param heapRelativeOffset offset relative to HEAP_START
   * @return absolute byte offset in the page
   */
  public static long heapAbsoluteOffset(final int heapRelativeOffset) {
    return HEAP_START + (long) heapRelativeOffset;
  }

  /**
   * Allocate space in the heap using bump allocation.
   * Updates the heapEnd in the page header.
   *
   * @param page the page MemorySegment
   * @param size number of bytes to allocate
   * @return heap-relative offset of the allocated region
   * @throws IllegalStateException if the page doesn't have enough space
   */
  public static int allocateHeap(final MemorySegment page, final int size) {
    final int heapEnd = getHeapEnd(page);
    final int newHeapEnd = heapEnd + size;

    // Check if we've exceeded the page capacity
    if (HEAP_START + newHeapEnd > page.byteSize()) {
      throw new IllegalStateException(
          "Heap overflow: need " + (HEAP_START + newHeapEnd) +
              " bytes but page is " + page.byteSize() + " bytes");
    }

    setHeapEnd(page, newHeapEnd);
    // Update heapUsed (for live data tracking — initially same as heapEnd)
    setHeapUsed(page, getHeapUsed(page) + size);
    return heapEnd;
  }

  /**
   * Get the remaining heap capacity in bytes.
   *
   * @param page the page MemorySegment
   * @return remaining bytes available for heap allocation
   */
  public static int heapCapacityRemaining(final MemorySegment page) {
    return (int) page.byteSize() - HEAP_START - getHeapEnd(page);
  }

  /**
   * Compute the heap fragmentation ratio.
   *
   * @return ratio of dead space to total allocated (0.0 = no fragmentation, 1.0 = all dead)
   */
  public static double heapFragmentation(final MemorySegment page) {
    final int heapEnd = getHeapEnd(page);
    if (heapEnd == 0) {
      return 0.0;
    }
    final int heapUsed = getHeapUsed(page);
    return 1.0 - ((double) heapUsed / heapEnd);
  }

  // ==================== PAGE INITIALIZATION ====================

  /**
   * Initialize a fresh page MemorySegment with default header values.
   * Zeroes the bitmap and directory regions.
   *
   * @param page           the page MemorySegment (must be at least INITIAL_PAGE_SIZE)
   * @param recordPageKey  the page key
   * @param revision       the revision number
   * @param indexType      the index type byte
   * @param areDeweyIDs    whether DeweyIDs will be stored
   */
  public static void initializePage(final MemorySegment page, final long recordPageKey,
      final int revision, final byte indexType, final boolean areDeweyIDs) {
    // Clear header + bitmap + directory (all zeros)
    page.asSlice(0, HEAP_START).fill((byte) 0);

    // Write header fields
    setRecordPageKey(page, recordPageKey);
    setRevision(page, revision);
    setPopulatedCount(page, 0);
    setHeapEnd(page, 0);
    setHeapUsed(page, 0);
    setIndexType(page, indexType);
    byte flags = 0;
    if (areDeweyIDs) {
      flags |= (byte) FLAG_DEWEY_IDS_STORED;
    }
    setFlags(page, flags);
  }

  // ==================== PAGE RESIZING ====================

  /**
   * Compute the new page size for growth (doubling strategy).
   *
   * @param currentSize the current page size
   * @param needed      the minimum total size needed
   * @return the new page size (at least needed, typically 2x current)
   */
  public static int computeGrowthSize(final int currentSize, final int needed) {
    int newSize = currentSize;
    while (newSize < needed) {
      newSize <<= 1; // double
    }
    return newSize;
  }

  // ==================== RECORD OFFSET TABLE ====================

  /**
   * Read a field offset from a record's offset table.
   * The offset table starts at recordBase + 1 (after the nodeKind byte).
   *
   * @param page         the page MemorySegment
   * @param recordBase   absolute byte offset of the record start
   * @param fieldIndex   the field index (0 to fieldCount-1)
   * @return the field offset (0-255) relative to the data region start
   */
  public static int readFieldOffset(final MemorySegment page, final long recordBase,
      final int fieldIndex) {
    return page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
  }

  /**
   * Write a field offset into a record's offset table.
   *
   * @param page        the page MemorySegment
   * @param recordBase  absolute byte offset of the record start
   * @param fieldIndex  the field index (0 to fieldCount-1)
   * @param offset      the field offset (0-255) relative to the data region start
   */
  public static void writeFieldOffset(final MemorySegment page, final long recordBase,
      final int fieldIndex, final int offset) {
    page.set(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex, (byte) offset);
  }

  /**
   * Compute the absolute offset of the data region for a record.
   * Data region starts after: [nodeKind: 1 byte] + [offset table: fieldCount bytes].
   *
   * @param recordBase absolute byte offset of the record start
   * @param fieldCount number of fields in the offset table
   * @return absolute byte offset where the data region begins
   */
  public static long dataRegionStart(final long recordBase, final int fieldCount) {
    return recordBase + 1 + fieldCount;
  }

  /**
   * Read the nodeKind byte from a record in the heap.
   *
   * @param page       the page MemorySegment
   * @param recordBase absolute byte offset of the record start
   * @return the nodeKind byte
   */
  public static byte readRecordKind(final MemorySegment page, final long recordBase) {
    return page.get(ValueLayout.JAVA_BYTE, recordBase);
  }

  /**
   * Write the nodeKind byte for a record in the heap.
   *
   * @param page       the page MemorySegment
   * @param recordBase absolute byte offset of the record start
   * @param kindId     the nodeKind byte
   */
  public static void writeRecordKind(final MemorySegment page, final long recordBase,
      final byte kindId) {
    page.set(ValueLayout.JAVA_BYTE, recordBase, kindId);
  }

  // ==================== DEWEY ID INLINE SUPPORT ====================

  /**
   * Size of the DeweyID length trailer appended to each heap allocation
   * when FLAG_DEWEY_IDS_STORED is set. Unsigned 16-bit (0-65535).
   */
  public static final int DEWEY_ID_TRAILER_SIZE = 2;

  /**
   * Read the DeweyID length from the trailer at the end of a slot's heap allocation.
   * Returns 0 if DeweyIDs are not stored or the slot has no DeweyID.
   *
   * @param page      the page MemorySegment
   * @param slotIndex the slot index
   * @return DeweyID data length in bytes (0 if none)
   */
  public static int getDeweyIdLength(final MemorySegment page, final int slotIndex) {
    if (!areDeweyIDsStored(page)) {
      return 0;
    }
    final int dataLength = getDirDataLength(page, slotIndex);
    if (dataLength < DEWEY_ID_TRAILER_SIZE) {
      return 0;
    }
    final int heapOffset = getDirHeapOffset(page, slotIndex);
    final long trailerPos = heapAbsoluteOffset(heapOffset) + dataLength - DEWEY_ID_TRAILER_SIZE;
    return Short.toUnsignedInt(page.get(JAVA_SHORT_UNALIGNED, trailerPos));
  }

  /**
   * Get the record-only data length (excluding DeweyID data and trailer).
   * When DeweyIDs are not stored, this equals the full dataLength.
   *
   * @param page      the page MemorySegment
   * @param slotIndex the slot index
   * @return record-only byte length
   */
  public static int getRecordOnlyLength(final MemorySegment page, final int slotIndex) {
    final int dataLength = getDirDataLength(page, slotIndex);
    if (!areDeweyIDsStored(page)) {
      return dataLength;
    }
    if (dataLength < DEWEY_ID_TRAILER_SIZE) {
      return dataLength;
    }
    final int deweyIdLen = getDeweyIdLength(page, slotIndex);
    return dataLength - deweyIdLen - DEWEY_ID_TRAILER_SIZE;
  }

  /**
   * Get the DeweyID data from a slot's heap allocation as a MemorySegment slice.
   * Returns null if the slot has no DeweyID.
   *
   * @param page      the page MemorySegment
   * @param slotIndex the slot index
   * @return MemorySegment slice containing DeweyID bytes, or null
   */
  public static MemorySegment getDeweyId(final MemorySegment page, final int slotIndex) {
    final int deweyIdLen = getDeweyIdLength(page, slotIndex);
    if (deweyIdLen == 0) {
      return null;
    }
    final int dataLength = getDirDataLength(page, slotIndex);
    final int heapOffset = getDirHeapOffset(page, slotIndex);
    // DeweyID data is between record data and the 2-byte trailer
    final long deweyIdStart = heapAbsoluteOffset(heapOffset) + dataLength
        - DEWEY_ID_TRAILER_SIZE - deweyIdLen;
    return page.asSlice(deweyIdStart, deweyIdLen);
  }

  /**
   * Write a 2-byte DeweyID length trailer (u16) at the end of a heap allocation.
   *
   * @param page       the page MemorySegment
   * @param absEnd     absolute byte position of the allocation end (where trailer goes)
   * @param deweyIdLen length of the DeweyID data (0 if none)
   */
  public static void writeDeweyIdTrailer(final MemorySegment page, final long absEnd,
      final int deweyIdLen) {
    page.set(JAVA_SHORT_UNALIGNED, absEnd - DEWEY_ID_TRAILER_SIZE, (short) deweyIdLen);
  }

  // ==================== COMPACTION ====================

  /**
   * Check whether the heap needs compaction.
   * Returns true when dead space exceeds the compaction threshold (25%).
   */
  public static boolean needsCompaction(final MemorySegment page) {
    return heapFragmentation(page) > 0.25;
  }
}
