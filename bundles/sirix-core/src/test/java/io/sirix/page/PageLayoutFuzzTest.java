package io.sirix.page;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fuzz tests for {@link PageLayout} (slotted page format).
 *
 * <p>Tests the page header, bitmap, directory, heap allocation,
 * and DeweyID trailer under random workloads. On failure the seed
 * is printed for reproducibility.
 */
class PageLayoutFuzzTest {

  // ==================== HEADER ROUNDTRIP FUZZ ====================

  @RepeatedTest(100)
  void fuzzHeaderFieldRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);

      final long recordPageKey = rng.nextLong();
      final int revision = rng.nextInt(Integer.MAX_VALUE);
      final byte indexType = (byte) rng.nextInt(256);
      final boolean deweyIDs = rng.nextBoolean();

      PageLayout.initializePage(page, recordPageKey, revision, indexType, deweyIDs);

      assertEquals(recordPageKey, PageLayout.getRecordPageKey(page),
          "recordPageKey [seed=" + seed + "]");
      assertEquals(revision, PageLayout.getRevision(page),
          "revision [seed=" + seed + "]");
      assertEquals(0, PageLayout.getPopulatedCount(page),
          "populatedCount should be 0 after init [seed=" + seed + "]");
      assertEquals(0, PageLayout.getHeapEnd(page),
          "heapEnd should be 0 after init [seed=" + seed + "]");
      assertEquals(0, PageLayout.getHeapUsed(page),
          "heapUsed should be 0 after init [seed=" + seed + "]");
      assertEquals(indexType, PageLayout.getIndexType(page),
          "indexType [seed=" + seed + "]");
      assertEquals(deweyIDs, PageLayout.areDeweyIDsStored(page),
          "deweyIDs flag [seed=" + seed + "]");

      // Mutate and re-read
      final long newKey = rng.nextLong();
      PageLayout.setRecordPageKey(page, newKey);
      assertEquals(newKey, PageLayout.getRecordPageKey(page),
          "recordPageKey after set [seed=" + seed + "]");

      final int newRev = rng.nextInt(Integer.MAX_VALUE);
      PageLayout.setRevision(page, newRev);
      assertEquals(newRev, PageLayout.getRevision(page),
          "revision after set [seed=" + seed + "]");

      final int newCount = rng.nextInt(PageLayout.SLOT_COUNT + 1);
      PageLayout.setPopulatedCount(page, newCount);
      assertEquals(newCount, PageLayout.getPopulatedCount(page),
          "populatedCount after set [seed=" + seed + "]");
    }
  }

  // ==================== BITMAP FUZZ ====================

  @RepeatedTest(100)
  void fuzzBitmapPopulateAndClear() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      // Track which slots we populate
      final boolean[] populated = new boolean[PageLayout.SLOT_COUNT];
      final int opsCount = rng.nextInt(500) + 50;

      for (int op = 0; op < opsCount; op++) {
        final int slot = rng.nextInt(PageLayout.SLOT_COUNT);

        if (rng.nextBoolean()) {
          // Populate
          PageLayout.markSlotPopulated(page, slot);
          populated[slot] = true;
        } else {
          // Clear
          PageLayout.clearSlotPopulated(page, slot);
          populated[slot] = false;
        }
      }

      // Verify all slots
      int expectedCount = 0;
      for (int i = 0; i < PageLayout.SLOT_COUNT; i++) {
        assertEquals(populated[i], PageLayout.isSlotPopulated(page, i),
            "Bitmap mismatch at slot " + i + " [seed=" + seed + "]");
        if (populated[i]) {
          expectedCount++;
        }
      }

      assertEquals(expectedCount, PageLayout.countPopulatedSlots(page),
          "countPopulatedSlots mismatch [seed=" + seed + "]");
    }
  }

  @RepeatedTest(50)
  void fuzzBitmapCopyRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      // Randomly populate slots
      final int numSlots = rng.nextInt(PageLayout.SLOT_COUNT);
      for (int i = 0; i < numSlots; i++) {
        PageLayout.markSlotPopulated(page, rng.nextInt(PageLayout.SLOT_COUNT));
      }

      // Copy bitmap to array
      final long[] bitmapCopy = new long[PageLayout.BITMAP_WORDS];
      PageLayout.copyBitmapTo(page, bitmapCopy);

      // Create a new page and copy bitmap back
      final MemorySegment page2 = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page2, 0, 0, (byte) 0, false);
      PageLayout.copyBitmapFrom(page2, bitmapCopy);

      // Compare all bitmap words
      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        assertEquals(
            PageLayout.getBitmapWord(page, w),
            PageLayout.getBitmapWord(page2, w),
            "Bitmap word " + w + " mismatch after copy [seed=" + seed + "]");
      }
    }
  }

  // ==================== DIRECTORY ENTRY FUZZ ====================

  @RepeatedTest(100)
  void fuzzDirectoryEntryRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      final int numEntries = rng.nextInt(PageLayout.SLOT_COUNT) + 1;
      final int[] heapOffsets = new int[numEntries];
      final int[] dataLengths = new int[numEntries];
      final int[] nodeKindIds = new int[numEntries];
      final int[] slotIndices = new int[numEntries];

      // Generate unique random slot indices
      final boolean[] used = new boolean[PageLayout.SLOT_COUNT];
      for (int i = 0; i < numEntries; i++) {
        int slot;
        do {
          slot = rng.nextInt(PageLayout.SLOT_COUNT);
        } while (used[slot]);
        used[slot] = true;
        slotIndices[i] = slot;

        heapOffsets[i] = rng.nextInt(1_000_000);
        dataLengths[i] = rng.nextInt(0xFFFFFF); // max 3-byte unsigned
        nodeKindIds[i] = rng.nextInt(256);

        PageLayout.setDirEntry(page, slot, heapOffsets[i], dataLengths[i], nodeKindIds[i]);
      }

      // Read back and verify
      for (int i = 0; i < numEntries; i++) {
        final int slot = slotIndices[i];

        assertEquals(heapOffsets[i], PageLayout.getDirHeapOffset(page, slot),
            "heapOffset mismatch at slot " + slot + " [seed=" + seed + "]");
        assertEquals(dataLengths[i], PageLayout.getDirDataLength(page, slot),
            "dataLength mismatch at slot " + slot + " [seed=" + seed + "]");
        assertEquals(nodeKindIds[i], PageLayout.getDirNodeKindId(page, slot),
            "nodeKindId mismatch at slot " + slot + " [seed=" + seed + "]");
      }
    }
  }

  @RepeatedTest(50)
  void fuzzClearDirEntry() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      final int slot = rng.nextInt(PageLayout.SLOT_COUNT);
      PageLayout.setDirEntry(page, slot, 12345, 6789, 42);
      PageLayout.clearDirEntry(page, slot);

      assertEquals(0, PageLayout.getDirHeapOffset(page, slot),
          "heapOffset not zeroed [seed=" + seed + "]");
      assertEquals(0, PageLayout.getDirDataLength(page, slot),
          "dataLength not zeroed [seed=" + seed + "]");
      assertEquals(0, PageLayout.getDirNodeKindId(page, slot),
          "nodeKindId not zeroed [seed=" + seed + "]");
    }
  }

  // ==================== COMPACT DIRECTORY PACKING FUZZ ====================

  @RepeatedTest(100)
  void fuzzCompactDirEntryPacking() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int dataLength = rng.nextInt(0xFFFFFF + 1); // 0..16777215
    final int nodeKindId = rng.nextInt(256);

    try {
      final int packed = PageLayout.packCompactDirEntry(dataLength, nodeKindId);

      assertEquals(dataLength, PageLayout.unpackDataLength(packed),
          "unpackDataLength mismatch [seed=" + seed + ", dataLength=" + dataLength + "]");
      assertEquals(nodeKindId, PageLayout.unpackNodeKindId(packed),
          "unpackNodeKindId mismatch [seed=" + seed + ", nodeKindId=" + nodeKindId + "]");
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + ", dataLength=" + dataLength
          + ", nodeKindId=" + nodeKindId + "]: " + e.getMessage(), e);
    }
  }

  @Test
  void compactDirEntryRejectsOversizedLength() {
    assertThrows(IllegalArgumentException.class,
        () -> PageLayout.packCompactDirEntry(0x1000000, 0)); // 16777216
    assertThrows(IllegalArgumentException.class,
        () -> PageLayout.packCompactDirEntry(Integer.MAX_VALUE, 0));
  }

  // ==================== HEAP ALLOCATION FUZZ ====================

  @RepeatedTest(100)
  void fuzzHeapAllocation() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      final int maxHeapSpace = PageLayout.INITIAL_PAGE_SIZE - PageLayout.HEAP_START;
      int totalAllocated = 0;
      int allocCount = 0;

      while (totalAllocated < maxHeapSpace - 100) {
        final int allocSize = rng.nextInt(500) + 1;
        if (totalAllocated + allocSize > maxHeapSpace) {
          break;
        }

        final int offset = PageLayout.allocateHeap(page, allocSize);
        assertEquals(totalAllocated, offset,
            "Heap allocation offset mismatch at alloc #" + allocCount + " [seed=" + seed + "]");

        totalAllocated += allocSize;
        allocCount++;

        assertEquals(totalAllocated, PageLayout.getHeapEnd(page),
            "heapEnd mismatch after alloc #" + allocCount + " [seed=" + seed + "]");
        assertEquals(totalAllocated, PageLayout.getHeapUsed(page),
            "heapUsed mismatch after alloc #" + allocCount + " [seed=" + seed + "]");
      }

      // Verify remaining capacity
      final int remaining = PageLayout.heapCapacityRemaining(page);
      assertEquals(maxHeapSpace - totalAllocated, remaining,
          "heapCapacityRemaining mismatch [seed=" + seed + "]");

      // Fresh page should have no fragmentation
      assertEquals(0.0, PageLayout.heapFragmentation(page), 0.0001,
          "Fresh page should have zero fragmentation [seed=" + seed + "]");
    }
  }

  @Test
  void heapAllocationRejectsOverflow() {
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      // Allocate nearly all space
      final int maxHeap = PageLayout.INITIAL_PAGE_SIZE - PageLayout.HEAP_START;
      PageLayout.allocateHeap(page, maxHeap - 10);

      // This should overflow
      assertThrows(IllegalStateException.class,
          () -> PageLayout.allocateHeap(page, 11));
    }
  }

  // ==================== FRAGMENTATION FUZZ ====================

  @RepeatedTest(50)
  void fuzzFragmentationCalculation() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      // Simulate allocations and some "dead" space
      final int totalAlloc = rng.nextInt(10000) + 100;
      final int maxHeap = PageLayout.INITIAL_PAGE_SIZE - PageLayout.HEAP_START;
      if (totalAlloc > maxHeap) {
        return; // skip if too large
      }

      PageLayout.setHeapEnd(page, totalAlloc);
      final int usedFraction = rng.nextInt(totalAlloc + 1);
      PageLayout.setHeapUsed(page, usedFraction);

      final double frag = PageLayout.heapFragmentation(page);
      final double expected = 1.0 - ((double) usedFraction / totalAlloc);

      assertEquals(expected, frag, 0.0001,
          "Fragmentation calculation [seed=" + seed
              + ", heapEnd=" + totalAlloc + ", heapUsed=" + usedFraction + "]");

      // needsCompaction should be true when frag > 0.25
      assertEquals(frag > 0.25, PageLayout.needsCompaction(page),
          "needsCompaction inconsistency [seed=" + seed + "]");
    }
  }

  // ==================== PAGE GROWTH FUZZ ====================

  @RepeatedTest(100)
  void fuzzComputeGrowthSize() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int currentSize = (rng.nextInt(10) + 1) * PageLayout.INITIAL_PAGE_SIZE;
    final int needed = currentSize + rng.nextInt(currentSize * 3);

    final int newSize = PageLayout.computeGrowthSize(currentSize, needed);

    assertTrue(newSize >= needed,
        "Growth size " + newSize + " < needed " + needed + " [seed=" + seed + "]");
    assertTrue(newSize >= currentSize,
        "Growth size " + newSize + " < current " + currentSize + " [seed=" + seed + "]");
    // Should be a power of 2 multiple of currentSize
    assertTrue(Integer.bitCount(newSize / currentSize) <= 1 || newSize == needed,
        "Growth size not a clean doubling [seed=" + seed + "]");
  }

  // ==================== PRESERVATION BITMAP FUZZ ====================

  @RepeatedTest(100)
  void fuzzPreservationBitmap() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      assertFalse(PageLayout.hasPreservedSlots(page),
          "Fresh page should have no preserved slots [seed=" + seed + "]");

      // Mark random slots preserved
      final boolean[] preserved = new boolean[PageLayout.SLOT_COUNT];
      final int numOps = rng.nextInt(200) + 10;

      for (int i = 0; i < numOps; i++) {
        final int slot = rng.nextInt(PageLayout.SLOT_COUNT);
        PageLayout.markSlotPreserved(page, slot);
        preserved[slot] = true;
      }

      // Verify
      boolean anyPreserved = false;
      for (int i = 0; i < PageLayout.SLOT_COUNT; i++) {
        assertEquals(preserved[i], PageLayout.isSlotPreserved(page, i),
            "Preservation mismatch at slot " + i + " [seed=" + seed + "]");
        if (preserved[i]) {
          anyPreserved = true;
        }
      }

      assertEquals(anyPreserved, PageLayout.hasPreservedSlots(page),
          "hasPreservedSlots mismatch [seed=" + seed + "]");

      // Clear and verify
      PageLayout.clearPreservationBitmap(page);
      assertFalse(PageLayout.hasPreservedSlots(page),
          "After clear, should have no preserved slots [seed=" + seed + "]");
      for (int i = 0; i < PageLayout.SLOT_COUNT; i++) {
        assertFalse(PageLayout.isSlotPreserved(page, i),
            "Slot " + i + " should not be preserved after clear [seed=" + seed + "]");
      }
    }
  }

  // ==================== RECORD OFFSET TABLE FUZZ ====================

  @RepeatedTest(100)
  void fuzzRecordFieldOffsets() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      // Allocate a record in the heap
      final int fieldCount = rng.nextInt(10) + 1;
      final int recordSize = 1 + fieldCount + rng.nextInt(200);
      final int heapOff = PageLayout.allocateHeap(page, recordSize);
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOff);

      // Write nodeKind
      final byte nodeKind = (byte) rng.nextInt(256);
      PageLayout.writeRecordKind(page, recordBase, nodeKind);

      // Write random field offsets
      final int[] offsets = new int[fieldCount];
      for (int i = 0; i < fieldCount; i++) {
        offsets[i] = rng.nextInt(256);
        PageLayout.writeFieldOffset(page, recordBase, i, offsets[i]);
      }

      // Read back
      assertEquals(nodeKind, PageLayout.readRecordKind(page, recordBase),
          "nodeKind mismatch [seed=" + seed + "]");

      for (int i = 0; i < fieldCount; i++) {
        assertEquals(offsets[i], PageLayout.readFieldOffset(page, recordBase, i),
            "Field offset mismatch at index " + i + " [seed=" + seed + "]");
      }

      // Verify dataRegionStart
      final long dataStart = PageLayout.dataRegionStart(recordBase, fieldCount);
      assertEquals(recordBase + 1 + fieldCount, dataStart,
          "dataRegionStart mismatch [seed=" + seed + "]");
    }
  }

  // ==================== DEWEY ID TRAILER FUZZ ====================

  @RepeatedTest(100)
  void fuzzDeweyIdTrailer() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      final byte flags = (byte) PageLayout.FLAG_DEWEY_IDS_STORED;
      PageLayout.initializePage(page, 0, 0, (byte) 0, true);

      final int slot = rng.nextInt(PageLayout.SLOT_COUNT);

      // Simulate a record with DeweyID
      final int recordLen = rng.nextInt(200) + 10;
      final int deweyIdLen = rng.nextInt(recordLen - PageLayout.DEWEY_ID_TRAILER_SIZE);
      final int totalLen = recordLen + deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE;

      final int heapOff = PageLayout.allocateHeap(page, totalLen);
      PageLayout.setDirEntry(page, slot, heapOff, totalLen, 1);
      PageLayout.markSlotPopulated(page, slot);

      // Write DeweyID data
      final byte[] deweyIdData = new byte[deweyIdLen];
      rng.nextBytes(deweyIdData);
      final long deweyIdStart = PageLayout.heapAbsoluteOffset(heapOff) + totalLen
          - PageLayout.DEWEY_ID_TRAILER_SIZE - deweyIdLen;
      for (int i = 0; i < deweyIdLen; i++) {
        page.set(java.lang.foreign.ValueLayout.JAVA_BYTE, deweyIdStart + i, deweyIdData[i]);
      }

      // Write trailer
      final long absEnd = PageLayout.heapAbsoluteOffset(heapOff) + totalLen;
      PageLayout.writeDeweyIdTrailer(page, absEnd, deweyIdLen);

      // Read back
      assertEquals(deweyIdLen, PageLayout.getDeweyIdLength(page, slot),
          "DeweyID length mismatch [seed=" + seed + "]");

      final int recordOnlyLen = PageLayout.getRecordOnlyLength(page, slot);
      assertEquals(totalLen - deweyIdLen - PageLayout.DEWEY_ID_TRAILER_SIZE, recordOnlyLen,
          "recordOnlyLength mismatch [seed=" + seed + "]");

      if (deweyIdLen > 0) {
        final MemorySegment deweySlice = PageLayout.getDeweyId(page, slot);
        assertEquals(deweyIdLen, deweySlice.byteSize(),
            "DeweyID slice size mismatch [seed=" + seed + "]");

        for (int i = 0; i < deweyIdLen; i++) {
          assertEquals(deweyIdData[i],
              deweySlice.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i),
              "DeweyID byte " + i + " mismatch [seed=" + seed + "]");
        }
      }
    }
  }

  // ==================== FULL SLOT LIFECYCLE FUZZ ====================

  @RepeatedTest(50)
  void fuzzFullSlotLifecycle() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, rng.nextLong(), rng.nextInt(1000), (byte) 0, false);

      final int numSlots = rng.nextInt(50) + 5;
      final int[] slots = new int[numSlots];
      final int[] heapOffsets = new int[numSlots];
      final int[] dataLens = new int[numSlots];
      final byte[][] recordData = new byte[numSlots][];

      // Allocate and populate slots
      for (int i = 0; i < numSlots; i++) {
        slots[i] = i; // sequential slots for simplicity
        final int dataLen = rng.nextInt(100) + 5;
        dataLens[i] = dataLen;
        recordData[i] = new byte[dataLen];
        rng.nextBytes(recordData[i]);

        heapOffsets[i] = PageLayout.allocateHeap(page, dataLen);
        PageLayout.setDirEntry(page, slots[i], heapOffsets[i], dataLen, rng.nextInt(20));
        PageLayout.markSlotPopulated(page, slots[i]);

        // Write record data to heap
        final long absOff = PageLayout.heapAbsoluteOffset(heapOffsets[i]);
        for (int b = 0; b < dataLen; b++) {
          page.set(java.lang.foreign.ValueLayout.JAVA_BYTE, absOff + b, recordData[i][b]);
        }
      }

      // Verify all slots
      assertEquals(numSlots, PageLayout.countPopulatedSlots(page),
          "populatedSlots count [seed=" + seed + "]");

      for (int i = 0; i < numSlots; i++) {
        assertTrue(PageLayout.isSlotPopulated(page, slots[i]),
            "Slot " + slots[i] + " not populated [seed=" + seed + "]");
        assertEquals(heapOffsets[i], PageLayout.getDirHeapOffset(page, slots[i]),
            "heapOffset for slot " + slots[i] + " [seed=" + seed + "]");
        assertEquals(dataLens[i], PageLayout.getDirDataLength(page, slots[i]),
            "dataLength for slot " + slots[i] + " [seed=" + seed + "]");

        // Verify data integrity
        final long absOff = PageLayout.heapAbsoluteOffset(heapOffsets[i]);
        for (int b = 0; b < dataLens[i]; b++) {
          assertEquals(recordData[i][b],
              page.get(java.lang.foreign.ValueLayout.JAVA_BYTE, absOff + b),
              "Data byte " + b + " at slot " + slots[i] + " [seed=" + seed + "]");
        }
      }

      // Remove some slots
      final int toRemove = rng.nextInt(numSlots / 2) + 1;
      for (int i = 0; i < toRemove; i++) {
        final int slot = slots[i];
        PageLayout.clearSlotPopulated(page, slot);
        PageLayout.clearDirEntry(page, slot);
      }

      assertEquals(numSlots - toRemove, PageLayout.countPopulatedSlots(page),
          "populatedSlots after removal [seed=" + seed + "]");

      // Remaining slots should still have correct data
      for (int i = toRemove; i < numSlots; i++) {
        assertTrue(PageLayout.isSlotPopulated(page, slots[i]),
            "Remaining slot " + slots[i] + " not populated [seed=" + seed + "]");
        assertEquals(dataLens[i], PageLayout.getDirDataLength(page, slots[i]),
            "Remaining slot dataLength [seed=" + seed + "]");
      }
    }
  }

  // ==================== FLAGS FUZZ ====================

  @RepeatedTest(50)
  void fuzzFlagsReadWrite() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.INITIAL_PAGE_SIZE);
      PageLayout.initializePage(page, 0, 0, (byte) 0, false);

      final byte flags = (byte) rng.nextInt(256);
      PageLayout.setFlags(page, flags);
      assertEquals(flags, PageLayout.getFlags(page),
          "Flags read/write mismatch [seed=" + seed + "]");

      // Test individual flag bits
      final boolean expectDewey = (flags & PageLayout.FLAG_DEWEY_IDS_STORED) != 0;
      final boolean expectFsst = (flags & PageLayout.FLAG_HAS_FSST_TABLE) != 0;
      assertEquals(expectDewey, PageLayout.areDeweyIDsStored(page),
          "DeweyID flag mismatch [seed=" + seed + "]");
      assertEquals(expectFsst, PageLayout.hasFsstTable(page),
          "FSST flag mismatch [seed=" + seed + "]");
    }
  }

  // ==================== DIRECTORY ENTRY OFFSET CALCULATION ====================

  @Test
  void dirEntryOffsetCalculation() {
    for (int i = 0; i < PageLayout.SLOT_COUNT; i++) {
      final long expected = PageLayout.DIR_OFF + (long) i * PageLayout.DIR_ENTRY_SIZE;
      assertEquals(expected, PageLayout.dirEntryOffset(i),
          "dirEntryOffset(" + i + ")");
    }
  }

  @Test
  void heapAbsoluteOffsetCalculation() {
    assertEquals(PageLayout.HEAP_START, PageLayout.heapAbsoluteOffset(0));
    assertEquals(PageLayout.HEAP_START + 100, PageLayout.heapAbsoluteOffset(100));
    assertEquals(PageLayout.HEAP_START + 65535, PageLayout.heapAbsoluteOffset(65535));
  }
}
