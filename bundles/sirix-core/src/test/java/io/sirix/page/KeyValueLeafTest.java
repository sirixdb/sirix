package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.json.BooleanNode;
import io.sirix.settings.Constants;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueLeafPageTest {

  private KeyValueLeafPage keyValueLeafPage;

  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    long recordPageKey = 1L;

    keyValueLeafPage = new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").build(), 1, arena.allocate(SIXTYFOUR_KB), null);
  }

  @AfterEach
  void tearDown() {
    // close() will handle memory properly - it knows memory was externally allocated via Arena
    if (keyValueLeafPage != null) {
      keyValueLeafPage.close();
      keyValueLeafPage = null;
    }
    if (arena != null) {
      arena.close();
      arena = null;
    }
  }

  @Test
  void testLastSlotIndexAfterSingleInsertion() {
    byte[] data = new byte[] {1, 2, 3, 4};
    keyValueLeafPage.setSlot(data, 0);

    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after inserting one slot.");
  }

  @Test
  void testLastSlotIndexAfterMultipleInsertions() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8};

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);

    assertEquals(1, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 1 after inserting two slots.");
  }

  @Test
  void testLastSlotIndexAfterMiddleSlotUpdate() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8};
    byte[] data3 = new byte[] {9, 10, 11, 12};

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);
    keyValueLeafPage.setSlot(data3, 2);

    keyValueLeafPage.setSlot(new byte[] {13, 14}, 1); // Update the second slot

    assertEquals(2, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should remain 2 after updating the middle slot.");
  }

  @Test
  void testLastSlotIndexAfterSequentialInsertions() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8};
    byte[] data3 = new byte[] {9, 10, 11, 12};

    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after the first insertion.");

    keyValueLeafPage.setSlot(data2, 1);
    assertEquals(1, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 1 after the second insertion.");

    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 2 after the third insertion.");
  }

  @Test
  void testLastSlotIndexWithGapsInInsertions() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data3 = new byte[] {9, 10, 11, 12};

    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after inserting at index 0.");

    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should be 2 after inserting at index 2, with a gap at index 1.");
  }

  @Test
  void testLastSlotIndexAfterOutOfOrderInsertions() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8};
    byte[] data3 = new byte[] {9, 10, 11, 12};

    // Insert data at different indices
    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 2 after inserting at index 2.");

    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should be updated to 0 after inserting at index 0.");

    keyValueLeafPage.setSlot(data2, 2);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should remain 0 after setting a new slot at index 2.");

    // Insert at a new higher index
    keyValueLeafPage.setSlot(new byte[] {21, 22, 23, 24}, 3);
    assertEquals(3, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 3 after inserting at index 3.");
  }

  @Test
  void testLastSlotIndexAfterUpdates() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8};
    byte[] data3 = new byte[] {9, 10, 11, 12};

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);
    keyValueLeafPage.setSlot(data3, 2);

    assertEquals(2, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should be 2 after three sequential insertions.");

    // Update the slot at index 0
    keyValueLeafPage.setSlot(new byte[] {13, 14, 15, 16}, 0);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should remain 2 after updating the slot at index 0.");

    // Update the slot at index 2
    keyValueLeafPage.setSlot(new byte[] {17, 18, 19, 20}, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should remain 2 after updating the slot at index 2.");
  }

  @Test
  void testLastSlotIndexAfterInserts() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8};
    byte[] data3 = new byte[] {9, 10, 11, 12};

    // Insert slots sequentially at indices 0, 1, and 2
    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after inserting at index 0.");

    keyValueLeafPage.setSlot(data2, 1);
    assertEquals(1, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 1 after inserting at index 1.");

    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 2 after inserting at index 2.");

    // Insert a new slot at index 3
    byte[] data4 = new byte[] {13, 14, 15, 16};
    keyValueLeafPage.setSlot(data4, 3);
    assertEquals(3, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 3 after inserting at index 3.");

    // Update the slot at index 2
    byte[] updatedData3 = new byte[] {17, 18, 19, 20};
    keyValueLeafPage.setSlot(updatedData3, 2);
    assertEquals(3, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should remain 3 after updating the slot at index 2.");

    // Insert another slot at index 4
    byte[] data5 = new byte[] {21, 22, 23, 24};
    keyValueLeafPage.setSlot(data5, 4);
    assertEquals(4, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 4 after inserting at index 4.");
  }

  @Test
  void testLastSlotIndexWithMultipleUpdates() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8};
    byte[] data3 = new byte[] {9, 10, 11, 12};

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);
    keyValueLeafPage.setSlot(data3, 2);

    assertEquals(2, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should be 2 after inserting three slots.");

    // Update all slots
    keyValueLeafPage.setSlot(new byte[] {13, 14, 15, 16}, 0);
    keyValueLeafPage.setSlot(new byte[] {17, 18, 19, 20}, 1);
    keyValueLeafPage.setSlot(new byte[] {21, 22, 23, 24}, 2);

    assertEquals(2, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should remain 2 after updating all slots.");
  }

  @Test
  void testLastSlotIndexAfterInsertionAtHighIndex() {
    byte[] data1 = new byte[] {1, 2, 3, 4};

    keyValueLeafPage.setSlot(data1, 10);

    assertEquals(10, keyValueLeafPage.getLastSlotIndex(),
        "The last slot index should be 10 after inserting at index 10.");
  }

  @Test
  void testSetSlotWithoutShifting() {
    byte[] data = new byte[] {1, 2, 3, 4};
    keyValueLeafPage.setSlot(data, 0);

    MemorySegment slot = keyValueLeafPage.getSlot(0);
    assertNotNull(slot);
    assertEquals(4, slot.byteSize());
    assertArrayEquals(data, slot.toArray(ValueLayout.JAVA_BYTE));
  }

  @Test
  void testSetSlotWithShiftingRight() {
    byte[] data1 = new byte[] {1, 2, 3, 4};
    byte[] data2 = new byte[] {5, 6, 7, 8, 9, 10};

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);

    MemorySegment slot0 = keyValueLeafPage.getSlot(0);
    MemorySegment slot1 = keyValueLeafPage.getSlot(1);

    assertNotNull(slot0);
    assertNotNull(slot1);

    assertArrayEquals(data1, slot0.toArray(ValueLayout.JAVA_BYTE));
    assertArrayEquals(data2, slot1.toArray(ValueLayout.JAVA_BYTE));
  }

  @Test
  void testSetSlotWithShiftingLeft() {
    byte[] data1 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    byte[] data2 = new byte[] {9, 10};
    byte[] data3 = new byte[] {11, 12, 13};

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);

    // Now set a smaller data in the first slot to force a left shift
    keyValueLeafPage.setSlot(data3, 0);

    MemorySegment slot0 = keyValueLeafPage.getSlot(0);
    MemorySegment slot1 = keyValueLeafPage.getSlot(1);

    assertNotNull(slot0);
    assertNotNull(slot1);

    assertArrayEquals(data3, slot0.toArray(ValueLayout.JAVA_BYTE));
    assertArrayEquals(data2, slot1.toArray(ValueLayout.JAVA_BYTE));
  }

  @Test
  void testSetSlotWithResizing() {
    byte[] data = new byte[Constants.MAX_RECORD_SIZE]; // Use max size for testing resizing
    keyValueLeafPage.setSlot(data, 0);

    MemorySegment slot = keyValueLeafPage.getSlot(0);
    assertNotNull(slot);
    assertEquals(Constants.MAX_RECORD_SIZE, slot.byteSize());
    assertArrayEquals(data, slot.toArray(ValueLayout.JAVA_BYTE));
  }

  @Test
  void testDeweyIdOperations() {
    byte[] deweyId1 = new byte[] {1, 2, 3};
    byte[] deweyId2 = new byte[] {4, 5};

    try (var deweyIdArena = Arena.ofConfined()) {
      long recordPageKey = 1L;
      // Use local variable instead of overwriting the field
      KeyValueLeafPage deweyIdPage = new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT,
          new ResourceConfiguration.Builder("testResource").useDeweyIDs(true).build(), 1,
          deweyIdArena.allocate(SIXTYFOUR_KB), deweyIdArena.allocate(SIXTYFOUR_KB));

      deweyIdPage.setDeweyId(deweyId1, 0);
      deweyIdPage.setDeweyId(deweyId2, 1);

      MemorySegment segment1 = deweyIdPage.getDeweyId(0);
      MemorySegment segment2 = deweyIdPage.getDeweyId(1);

      assertNotNull(segment1);
      assertNotNull(segment2);

      assertArrayEquals(deweyId1, segment1.toArray(ValueLayout.JAVA_BYTE));
      assertArrayEquals(deweyId2, segment2.toArray(ValueLayout.JAVA_BYTE));

      deweyIdPage.close();
    }
  }

  @Test
  void testSetSlotMemorySegmentResizing() {
    // Raw slots are capped at MAX_RECORD_SIZE — anything above it must arrive as a canonical
    // overflow carrier, never as raw slotted-page bytes (setSlotWithNodeKind refuses). Heap growth
    // is therefore exercised the way production reaches it: many maximum-size records, which carry
    // the initial 64 KiB segment past its capacity and force growSlottedPage.
    final byte[] recordBytes = new byte[Constants.MAX_RECORD_SIZE];
    for (int slot = 0; slot < 200; slot++) {
      recordBytes[0] = (byte) slot;
      keyValueLeafPage.setSlot(MemorySegment.ofArray(recordBytes), slot);
    }
    for (int slot = 0; slot < 200; slot++) {
      final MemorySegment stored = keyValueLeafPage.getSlot(slot);
      assertNotNull(stored, "slot " + slot + " lost by heap growth");
      assertEquals(Constants.MAX_RECORD_SIZE, stored.byteSize());
      assertEquals((byte) slot, stored.get(ValueLayout.JAVA_BYTE, 0), "slot " + slot + " content after growth");
    }
    // And the cap itself is the contract, not an accident: one byte past it is refused.
    final MemorySegment oversized = MemorySegment.ofArray(new byte[Constants.MAX_RECORD_SIZE + 1]);
    assertThrows(IllegalArgumentException.class, () -> keyValueLeafPage.setSlot(oversized, 200),
        "raw slotted-page bytes above MAX_RECORD_SIZE must be refused");
  }

  @Test
  void testAddReferencesCopiesOnlyMarkedPreservationSlots() {
    byte[] preserved = new byte[] {42, 43, 44};

    try (var localArena = Arena.ofConfined()) {
      ResourceConfiguration config = new ResourceConfiguration.Builder("testResource").build();
      KeyValueLeafPage completePage =
          new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, localArena.allocate(SIXTYFOUR_KB), null);
      KeyValueLeafPage modifiedPage =
          new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, localArena.allocate(SIXTYFOUR_KB), null);

      completePage.setSlot(preserved, 7);
      modifiedPage.markSlotForPreservation(7);
      modifiedPage.setCompletePageRef(completePage);

      modifiedPage.addReferences(config);

      MemorySegment copied = modifiedPage.getSlot(7);
      assertNotNull(copied);
      assertArrayEquals(preserved, copied.toArray(ValueLayout.JAVA_BYTE));

      completePage.close();
      modifiedPage.close();
    }
  }

  @Test
  void testAddReferencesDoesNotOverwriteExistingModifiedSlot() {
    byte[] completeValue = new byte[] {1, 2, 3};
    byte[] modifiedValue = new byte[] {9, 8, 7};

    try (var localArena = Arena.ofConfined()) {
      ResourceConfiguration config = new ResourceConfiguration.Builder("testResource").build();
      KeyValueLeafPage completePage =
          new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, localArena.allocate(SIXTYFOUR_KB), null);
      KeyValueLeafPage modifiedPage =
          new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, localArena.allocate(SIXTYFOUR_KB), null);

      completePage.setSlot(completeValue, 11);
      modifiedPage.setSlot(modifiedValue, 11);
      modifiedPage.markSlotForPreservation(11);
      modifiedPage.setCompletePageRef(completePage);

      modifiedPage.addReferences(config);

      MemorySegment value = modifiedPage.getSlot(11);
      assertNotNull(value);
      assertArrayEquals(modifiedValue, value.toArray(ValueLayout.JAVA_BYTE));

      completePage.close();
      modifiedPage.close();
    }
  }

  @Test
  void testAddReferencesDoesNotOverwriteNewerSideCarrierInPreservedSlot() {
    final int slot = 11;
    final long recordKey = (1L << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
    final byte[] olderInlineValue = new byte[] {1, 2, 3};
    final byte[] newerSideImage = new byte[] {9, 8, 0, 0};
    final byte[] newerOverflowValue = new byte[] {7, 6, 5, 4};

    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("preserved-side-add-references").useDeweyIDs(true).build();
    final KeyValueLeafPage completePage = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, null, null, false);
    final KeyValueLeafPage modifiedPage = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 2, null, null, false);
    try {
      completePage.setSlot(olderInlineValue, slot);
      modifiedPage.markSlotForPreservation(slot);
      modifiedPage.setCompletePageRef(completePage);

      final PageReference newerReference =
          installSideCarrier(modifiedPage, recordKey, slot, newerSideImage, newerOverflowValue);

      modifiedPage.addReferences(config);

      assertCurrentSideCarrier(modifiedPage, recordKey, slot, newerSideImage, newerReference);
    } finally {
      modifiedPage.close();
      completePage.close();
    }
  }

  @Test
  void testBareReferenceCannotShadowPreservedSideMetadataAndDeweyId() {
    final int slot = 12;
    final long recordKey = (1L << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
    // kind-zero complete image: one record byte, one Dewey byte, little-endian Dewey length trailer.
    final byte[] currentSideImage = new byte[] {9, 8, 1, 0};

    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("preserved-side-bare-reference").useDeweyIDs(true).build();
    final KeyValueLeafPage completePage = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, null, null, false);
    final KeyValueLeafPage modifiedPage = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 2, null, null, false);
    try {
      final PageReference currentReference =
          installSideCarrier(completePage, recordKey, slot, currentSideImage, new byte[] {4, 5, 6});
      modifiedPage.markSlotForPreservation(slot);
      modifiedPage.setCompletePageRef(completePage);

      final PageReference staleBareReference = new PageReference();
      staleBareReference.setPage(new OverflowPage(new byte[] {1, 2, 3}));
      modifiedPage.setPageReference(recordKey, staleBareReference);

      modifiedPage.addReferences(config);

      assertCurrentSideCarrier(modifiedPage, recordKey, slot, currentSideImage, currentReference);
      assertArrayEquals(new byte[] {8}, modifiedPage.getDeweyIdAsByteArray(slot),
          "the preserved side image must carry its Dewey metadata beside the winning reference");
    } finally {
      modifiedPage.close();
      completePage.close();
    }
  }

  @Test
  void testDeepCopyDoesNotPairNewerSideImageWithRestoredPreservedCarrier() {
    final int slot = 13;
    final long recordKey = (1L << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
    final byte[] olderInlineValue = new byte[] {1, 2, 3};
    final byte[] newerSideImage = new byte[] {9, 8, 0, 0};
    final byte[] newerOverflowValue = new byte[] {7, 6, 5, 4};

    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("preserved-side-deep-copy").useDeweyIDs(true).build();
    final KeyValueLeafPage completePage = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, null, null, false);
    final KeyValueLeafPage modifiedPage = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 2, null, null, false);
    KeyValueLeafPage copy = null;
    try {
      completePage.setSlot(olderInlineValue, slot);
      modifiedPage.markSlotForPreservation(slot);
      modifiedPage.setCompletePageRef(completePage);

      final PageReference newerReference =
          installSideCarrier(modifiedPage, recordKey, slot, newerSideImage, newerOverflowValue);

      copy = modifiedPage.deepCopy();

      final PageReference copiedReference = copy.getPageReference(recordKey);
      assertCurrentSideCarrier(copy, recordKey, slot, newerSideImage, copiedReference);
      assertNotSame(newerReference, copiedReference, "deep copy must retain an independently mutable PageReference");
      assertSame(newerReference.getPage(), copiedReference.getPage(),
          "a fresh unresolved reference retains the one immutable OverflowPage payload");
    } finally {
      if (copy != null) {
        copy.close();
      }
      modifiedPage.close();
      completePage.close();
    }
  }

  @Test
  void testLogicalSlotBitmapWordMergesLogicalCarriersWithoutDuplicates() {
    final int inlineSlot = 1;
    final int sideSlot = 2;
    final int referenceSlot = 3;
    final int pendingSlot = 4;
    final long pageKeyBase = keyValueLeafPage.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT;

    keyValueLeafPage.setSlot(new byte[] {1}, inlineSlot);
    final long sideToken = keyValueLeafPage.prepareSideSlot(0, MemorySegment.ofArray(new byte[] {2}), 1);
    keyValueLeafPage.publishSideSlot(sideSlot, sideToken);
    keyValueLeafPage.setPageReference(pageKeyBase + referenceSlot, new PageReference());

    // A pending record may temporarily coexist with an older inline carrier for the same slot.
    // The logical bitmap must expose that key only once while still including pending-only keys.
    keyValueLeafPage.setRecord(
        new BooleanNode(pageKeyBase + inlineSlot, 5L, 1, 2, 7L, 6L, 11L, true, LongHashFunction.xx3(), (byte[]) null));
    keyValueLeafPage.setRecord(new BooleanNode(pageKeyBase + pendingSlot, 5L, 1, 2, 7L, 6L, 11L, false,
        LongHashFunction.xx3(), (byte[]) null));

    final long foreignPageKey = (keyValueLeafPage.getPageKey() + 1) << Constants.NDP_NODE_COUNT_EXPONENT;
    keyValueLeafPage.setPageReference(foreignPageKey + 5, new PageReference());

    final long logicalWord = keyValueLeafPage.logicalSlotBitmapWord(0);
    final long expectedWord = 1L << inlineSlot | 1L << sideSlot | 1L << referenceSlot | 1L << pendingSlot;
    assertEquals(expectedWord, logicalWord);
    assertEquals(4, Long.bitCount(logicalWord), "overlapping pending and inline carriers must be deduplicated");
    assertThrows(IndexOutOfBoundsException.class, () -> keyValueLeafPage.logicalSlotBitmapWord(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> keyValueLeafPage.logicalSlotBitmapWord(16));
  }

  @Test
  void testForEachPopulatedSlotVisitsEveryLogicalCarrierOnceAndReportsEarlyStop() {
    final int inlineSlot = 1;
    final int sideSlot = 2;
    final int referenceSlot = 3;
    final int pendingSlot = 4;
    final long pageKeyBase = keyValueLeafPage.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT;

    keyValueLeafPage.setSlot(new byte[] {1}, inlineSlot);
    final long sideToken = keyValueLeafPage.prepareSideSlot(0, MemorySegment.ofArray(new byte[] {2}), 1);
    keyValueLeafPage.publishSideSlot(sideSlot, sideToken);
    keyValueLeafPage.setPageReference(pageKeyBase + referenceSlot, new PageReference());
    keyValueLeafPage.setRecord(
        new BooleanNode(pageKeyBase + inlineSlot, 5L, 1, 2, 7L, 6L, 11L, true, LongHashFunction.xx3(), (byte[]) null));
    keyValueLeafPage.setRecord(new BooleanNode(pageKeyBase + pendingSlot, 5L, 1, 2, 7L, 6L, 11L, false,
        LongHashFunction.xx3(), (byte[]) null));
    final long foreignPageKey = (keyValueLeafPage.getPageKey() + 1) << Constants.NDP_NODE_COUNT_EXPONENT;
    keyValueLeafPage.setPageReference(foreignPageKey + 5, new PageReference());

    final boolean[] visited = new boolean[Constants.NDP_NODE_COUNT];
    final int processed = keyValueLeafPage.forEachPopulatedSlot(slot -> {
      assertFalse(visited[slot], "logical slot " + slot + " was emitted more than once");
      visited[slot] = true;
      return true;
    });
    assertEquals(4, processed);
    assertTrue(visited[inlineSlot]);
    assertTrue(visited[sideSlot]);
    assertTrue(visited[referenceSlot]);
    assertTrue(visited[pendingSlot]);
    assertFalse(visited[5], "a reference owned by another record page must be excluded");

    final int[] callbacks = new int[1];
    final int stoppedAfter = keyValueLeafPage.forEachPopulatedSlot(slot -> ++callbacks[0] < 2);
    assertEquals(2, stoppedAfter, "processed count must include the slot which requested early stop");
    assertEquals(2, callbacks[0]);
    assertThrows(NullPointerException.class, () -> keyValueLeafPage.forEachPopulatedSlot(null));
  }

  private static PageReference installSideCarrier(final KeyValueLeafPage page, final long recordKey, final int slot,
      final byte[] sideImage, final byte[] overflowValue) {
    final long token = page.prepareSideSlot(0, MemorySegment.ofArray(sideImage), sideImage.length);
    page.publishSideSlot(slot, token);
    final PageReference reference = new PageReference();
    reference.setPage(new OverflowPage(overflowValue));
    page.setPageReference(recordKey, reference);
    return reference;
  }

  private static void assertCurrentSideCarrier(final KeyValueLeafPage page, final long recordKey, final int slot,
      final byte[] expectedImage, final PageReference expectedReference) {
    assertTrue(page.hasSideSlot(slot), "newer side image must remain authoritative");
    assertFalse(PageLayout.isSlotPopulated(page.getSlottedPage(), slot),
        "preserved inline bytes must not overlap a newer side image");
    assertNull(page.getSlot(slot));
    assertArrayEquals(expectedImage, page.getSideSlotImage(slot).toArray(ValueLayout.JAVA_BYTE));
    assertNotNull(expectedReference, "side image must retain a same-key overflow reference");
    assertSame(expectedReference, page.getPageReference(recordKey),
        "side image and same-key overflow reference must remain paired");
  }

  @Test
  void testAddReferencesCopiesPreservedSlotData() {
    final long nodeKey = 7L;
    final int offset =
        (int) (nodeKey - ((nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
    final BooleanNode node =
        new BooleanNode(nodeKey, 5L, 2, 3, 11L, 13L, 17L, true, LongHashFunction.xx3(), (byte[]) null);

    try (var localArena = Arena.ofConfined()) {
      final ResourceConfiguration config = new ResourceConfiguration.Builder("testResource").build();
      final KeyValueLeafPage completePage =
          new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, localArena.allocate(SIXTYFOUR_KB), null);
      final KeyValueLeafPage modifiedPage =
          new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, localArena.allocate(SIXTYFOUR_KB), null);

      completePage.setRecord(node);
      completePage.addReferences(config);

      modifiedPage.markSlotForPreservation(offset);
      modifiedPage.setCompletePageRef(completePage);
      modifiedPage.addReferences(config);

      final MemorySegment completeSlot = completePage.getSlot(offset);
      final MemorySegment modifiedSlot = modifiedPage.getSlot(offset);
      assertNotNull(completeSlot);
      assertNotNull(modifiedSlot);
      assertArrayEquals(completeSlot.toArray(ValueLayout.JAVA_BYTE), modifiedSlot.toArray(ValueLayout.JAVA_BYTE));

      completePage.close();
      modifiedPage.close();
    }
  }

  @Test
  void testSetRecordDefersSerializationForNonSingletonFlyweight() {
    final long nodeKey = 1L;
    final int offset =
        (int) (nodeKey - ((nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
    final BooleanNode node =
        new BooleanNode(nodeKey, 7L, 3, 4, 9L, 11L, 13L, true, LongHashFunction.xx3(), (byte[]) null);

    keyValueLeafPage.setRecord(node);

    // Non-singleton FlyweightNode: stored in records[] for processEntries serialization.
    // Only write singletons are serialized directly to heap.
    assertSame(node, keyValueLeafPage.getRecord(offset));
  }

  @Test
  void testRawSlotWriteWithoutMaterializedRecord() {
    final long nodeKey = 1L;
    final int offset =
        (int) (nodeKey - ((nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
    final BooleanNode node =
        new BooleanNode(nodeKey, 2L, 3, 4, 5L, 6L, 7L, false, LongHashFunction.xx3(), (byte[]) null);

    keyValueLeafPage.setRecord(node);

    keyValueLeafPage.records()[offset] = null;
    keyValueLeafPage.setSlot(new byte[] {1, 2, 3}, offset);

    final MemorySegment slot = keyValueLeafPage.getSlot(offset);
    assertNotNull(slot);
    assertArrayEquals(new byte[] {1, 2, 3}, slot.toArray(ValueLayout.JAVA_BYTE));
  }

}
