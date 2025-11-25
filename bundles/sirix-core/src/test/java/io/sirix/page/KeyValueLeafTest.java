package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.*;

class KeyValueLeafPageTest {

  private KeyValueLeafPage keyValueLeafPage;

  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    long recordPageKey = 1L;

    keyValueLeafPage = new KeyValueLeafPage(recordPageKey,
                                            IndexType.DOCUMENT,
                                            new ResourceConfiguration.Builder("testResource").build(),
                                            1,
                                            arena.allocate(SIXTYFOUR_KB),
                                            null);
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
    byte[] data = new byte[] { 1, 2, 3, 4 };
    keyValueLeafPage.setSlot(data, 0);

    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after inserting one slot.");
  }

  @Test
  void testLastSlotIndexAfterMultipleInsertions() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8 };

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);

    assertEquals(1, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 1 after inserting two slots.");
  }

  @Test
  void testLastSlotIndexAfterMiddleSlotUpdate() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8 };
    byte[] data3 = new byte[] { 9, 10, 11, 12 };

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);
    keyValueLeafPage.setSlot(data3, 2);

    keyValueLeafPage.setSlot(new byte[] { 13, 14 }, 1); // Update the second slot

    assertEquals(2,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should remain 2 after updating the middle slot.");
  }

  @Test
  void testLastSlotIndexAfterSequentialInsertions() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8 };
    byte[] data3 = new byte[] { 9, 10, 11, 12 };

    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after the first insertion.");

    keyValueLeafPage.setSlot(data2, 1);
    assertEquals(1, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 1 after the second insertion.");

    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 2 after the third insertion.");
  }

  @Test
  void testLastSlotIndexWithGapsInInsertions() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data3 = new byte[] { 9, 10, 11, 12 };

    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after inserting at index 0.");

    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should be 2 after inserting at index 2, with a gap at index 1.");
  }

  @Test
  void testLastSlotIndexAfterOutOfOrderInsertions() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8 };
    byte[] data3 = new byte[] { 9, 10, 11, 12 };

    // Insert data at different indices
    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 2 after inserting at index 2.");

    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should be updated to 0 after inserting at index 0.");

    keyValueLeafPage.setSlot(data2, 2);
    assertEquals(0,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should remain 0 after setting a new slot at index 2.");

    // Insert at a new higher index
    keyValueLeafPage.setSlot(new byte[] { 21, 22, 23, 24 }, 3);
    assertEquals(3, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 3 after inserting at index 3.");
  }

  @Test
  void testLastSlotIndexAfterUpdates() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8 };
    byte[] data3 = new byte[] { 9, 10, 11, 12 };

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);
    keyValueLeafPage.setSlot(data3, 2);

    assertEquals(2,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should be 2 after three sequential insertions.");

    // Update the slot at index 0
    keyValueLeafPage.setSlot(new byte[] { 13, 14, 15, 16 }, 0);
    assertEquals(2,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should remain 2 after updating the slot at index 0.");

    // Update the slot at index 2
    keyValueLeafPage.setSlot(new byte[] { 17, 18, 19, 20 }, 2);
    assertEquals(2,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should remain 2 after updating the slot at index 2.");
  }

  @Test
  void testLastSlotIndexAfterInserts() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8 };
    byte[] data3 = new byte[] { 9, 10, 11, 12 };

    // Insert slots sequentially at indices 0, 1, and 2
    keyValueLeafPage.setSlot(data1, 0);
    assertEquals(0, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 0 after inserting at index 0.");

    keyValueLeafPage.setSlot(data2, 1);
    assertEquals(1, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 1 after inserting at index 1.");

    keyValueLeafPage.setSlot(data3, 2);
    assertEquals(2, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 2 after inserting at index 2.");

    // Insert a new slot at index 3
    byte[] data4 = new byte[] { 13, 14, 15, 16 };
    keyValueLeafPage.setSlot(data4, 3);
    assertEquals(3, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 3 after inserting at index 3.");

    // Update the slot at index 2
    byte[] updatedData3 = new byte[] { 17, 18, 19, 20 };
    keyValueLeafPage.setSlot(updatedData3, 2);
    assertEquals(3,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should remain 3 after updating the slot at index 2.");

    // Insert another slot at index 4
    byte[] data5 = new byte[] { 21, 22, 23, 24 };
    keyValueLeafPage.setSlot(data5, 4);
    assertEquals(4, keyValueLeafPage.getLastSlotIndex(), "The last slot index should be 4 after inserting at index 4.");
  }

  @Test
  void testLastSlotIndexWithMultipleUpdates() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8 };
    byte[] data3 = new byte[] { 9, 10, 11, 12 };

    keyValueLeafPage.setSlot(data1, 0);
    keyValueLeafPage.setSlot(data2, 1);
    keyValueLeafPage.setSlot(data3, 2);

    assertEquals(2,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should be 2 after inserting three slots.");

    // Update all slots
    keyValueLeafPage.setSlot(new byte[] { 13, 14, 15, 16 }, 0);
    keyValueLeafPage.setSlot(new byte[] { 17, 18, 19, 20 }, 1);
    keyValueLeafPage.setSlot(new byte[] { 21, 22, 23, 24 }, 2);

    assertEquals(2,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should remain 2 after updating all slots.");
  }

  @Test
  void testLastSlotIndexAfterInsertionAtHighIndex() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };

    keyValueLeafPage.setSlot(data1, 10);

    assertEquals(10,
                 keyValueLeafPage.getLastSlotIndex(),
                 "The last slot index should be 10 after inserting at index 10.");
  }

  @Test
  void testSetSlotWithoutShifting() {
    byte[] data = new byte[] { 1, 2, 3, 4 };
    keyValueLeafPage.setSlot(data, 0);

    MemorySegment slot = keyValueLeafPage.getSlot(0);
    assertNotNull(slot);
    assertEquals(4, slot.byteSize());
    assertArrayEquals(data, slot.toArray(ValueLayout.JAVA_BYTE));
  }

  @Test
  void testSetSlotWithShiftingRight() {
    byte[] data1 = new byte[] { 1, 2, 3, 4 };
    byte[] data2 = new byte[] { 5, 6, 7, 8, 9, 10 };

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
    byte[] data1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
    byte[] data2 = new byte[] { 9, 10 };
    byte[] data3 = new byte[] { 11, 12, 13 };

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
    byte[] deweyId1 = new byte[] { 1, 2, 3 };
    byte[] deweyId2 = new byte[] { 4, 5 };

    try (var deweyIdArena = Arena.ofConfined()) {
      long recordPageKey = 1L;
      // Use local variable instead of overwriting the field
      KeyValueLeafPage deweyIdPage = new KeyValueLeafPage(recordPageKey,
                                              IndexType.DOCUMENT,
                                              new ResourceConfiguration.Builder("testResource").useDeweyIDs(true)
                                                                                               .build(),
                                              1,
                                              deweyIdArena.allocate(SIXTYFOUR_KB),
                                              deweyIdArena.allocate(SIXTYFOUR_KB));

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
    MemorySegment largeData = MemorySegment.ofArray(new byte[Constants.MAX_RECORD_SIZE * 2]);
    keyValueLeafPage.setSlot(largeData, 0);

    MemorySegment slot = keyValueLeafPage.getSlot(0);
    assertNotNull(slot);
    assertEquals(largeData.byteSize(), slot.byteSize());
  }

  @Test
  void testHasEnoughSpaceWithSufficientSpace() {
    // Simulate a situation where there is enough space for the new data.
    byte[] recordData = new byte[50];
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment memory = arena.allocate(200); // Create a memory segment of 200 bytes.

      // Set some initial data.
      keyValueLeafPage.setSlotMemory(memory);
      keyValueLeafPage.setSlot(recordData, 0);
      assertTrue(keyValueLeafPage.hasEnoughSpace(offsets, memory, 50),
                 "There should be enough space for the new data.");
    }
  }

  @Test
  void testHasEnoughSpaceWithExactSpace() {
    // Simulate a situation where there is exactly enough space.
    byte[] recordData = new byte[50];
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment memory = arena.allocate(106); // Create a memory segment of 106 bytes (100 + 4(size) + 2(aligned).

      // Set some initial data.
      keyValueLeafPage.setSlotMemory(memory);
      keyValueLeafPage.setSlot(recordData, 0);

      // Check that there's exactly enough space left.
      assertTrue(keyValueLeafPage.hasEnoughSpace(offsets, memory, 50),
                 "There should be exactly enough space for the new data.");
    }
  }

  @Test
  void testHasEnoughSpaceWithInsufficientSpace() {
    // Simulate a situation where there is not enough space for the new data.
    byte[] recordData = new byte[50];
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment memory = arena.allocate(80); // Create a memory segment of 80 bytes.

      // Set some initial data.
      keyValueLeafPage.setSlotMemory(memory);
      keyValueLeafPage.setSlot(recordData, 0);

      // Check that there's not enough space for new data.
      assertFalse(keyValueLeafPage.hasEnoughSpace(offsets, memory, 100),
                  "There should not be enough space for the new data.");
    }
  }

  @Test
  void testHasEnoughSpaceWithLargeOffset() {
    // Simulate a situation with large offset values and small memory.
    byte[] recordData = new byte[20];
    int[] offsets = new int[] { 0, 24, -1 };

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment memory = arena.allocate(50); // Small memory segment.

      keyValueLeafPage.setSlotMemory(memory);

      // Set some initial data.
      keyValueLeafPage.setSlot(recordData, 0);
      keyValueLeafPage.setSlot(recordData, 1);

      // Check that there's not enough space.
      assertFalse(keyValueLeafPage.hasEnoughSpace(offsets, memory, 50),
                  "There should not be enough space due to the small memory segment and large offsets.");
    }
  }

  @Test
  void testHasEnoughSpaceWithEmptyMemory() {
    // Simulate a situation with an empty memory segment.
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment memory = arena.allocate(0); // Empty memory segment.

      keyValueLeafPage.setSlotMemory(memory);
      // Check that there's not enough space.
      assertFalse(keyValueLeafPage.hasEnoughSpace(offsets, memory, 1),
                  "There should not be enough space with an empty memory segment.");
    }
  }

  @Test
  void testHasEnoughSpaceAfterResizing() {
    // Simulate a situation where the memory is resized.
    byte[] recordData = new byte[50];
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment memory = arena.allocate(54); // Small memory segment.

      // Set some initial data.
      keyValueLeafPage.setSlotMemory(memory);
      keyValueLeafPage.setSlot(recordData, 0);

      // Resize the memory segment.
      MemorySegment resizedMemory = arena.allocate(100);

      keyValueLeafPage.setSlotMemory(resizedMemory);

      // Check that there's enough space after resizing.
      assertTrue(keyValueLeafPage.hasEnoughSpace(offsets, resizedMemory, 50),
                 "There should be enough space after resizing the memory segment.");
    }
  }
}