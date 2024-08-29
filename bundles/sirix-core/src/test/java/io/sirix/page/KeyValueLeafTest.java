package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueLeafPageTest {

  private KeyValueLeafPage keyValueLeafPage;

  @BeforeEach
  void setUp() {
    long recordPageKey = 1L;
    keyValueLeafPage = new KeyValueLeafPage(recordPageKey,
                                            IndexType.DOCUMENT,
                                            new ResourceConfiguration.Builder("testResource").build(),
                                            1);
  }

  @Test
  void testSetSlotWithoutShifting() {
    byte[] data = new byte[]{1, 2, 3, 4};
    keyValueLeafPage.setSlot(data, 0);

    MemorySegment slot = keyValueLeafPage.getSlot(0);
    assertNotNull(slot);
    assertEquals(4, slot.byteSize());
    assertArrayEquals(data, slot.toArray(ValueLayout.JAVA_BYTE));
  }

  @Test
  void testSetSlotWithShiftingRight() {
    byte[] data1 = new byte[]{1, 2, 3, 4};
    byte[] data2 = new byte[]{5, 6, 7, 8, 9, 10};

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
    byte[] data1 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    byte[] data2 = new byte[]{9, 10};
    byte[] data3 = new byte[]{11, 12, 13};

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
    byte[] deweyId1 = new byte[]{1, 2, 3};
    byte[] deweyId2 = new byte[]{4, 5};

    keyValueLeafPage.setDeweyId(deweyId1, 0);
    keyValueLeafPage.setDeweyId(deweyId2, 1);

    MemorySegment segment1 = keyValueLeafPage.getDeweyId(0);
    MemorySegment segment2 = keyValueLeafPage.getDeweyId(1);

    assertNotNull(segment1);
    assertNotNull(segment2);

    assertArrayEquals(deweyId1, segment1.toArray(ValueLayout.JAVA_BYTE));
    assertArrayEquals(deweyId2, segment2.toArray(ValueLayout.JAVA_BYTE));
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
    MemorySegment memory = MemorySegment.ofArray(new byte[200]); // Create a memory segment of 200 bytes.

    // Set some initial data.
    keyValueLeafPage.setSlot(recordData, 0);
    assertTrue(keyValueLeafPage.hasEnoughSpace(offsets, memory, 50),
               "There should be enough space for the new data.");
  }

  @Test
  void testHasEnoughSpaceWithExactSpace() {
    // Simulate a situation where there is exactly enough space.
    byte[] recordData = new byte[50];
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.
    MemorySegment memory = MemorySegment.ofArray(new byte[100]); // Create a memory segment of 100 bytes.

    // Set some initial data.
    keyValueLeafPage.setSlot(recordData, 0);

    // Check that there's exactly enough space left.
    assertTrue(keyValueLeafPage.hasEnoughSpace(offsets, memory, 50),
               "There should be exactly enough space for the new data.");
  }

  @Test
  void testHasEnoughSpaceWithInsufficientSpace() {
    // Simulate a situation where there is not enough space for the new data.
    byte[] recordData = new byte[50];
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.
    MemorySegment memory = MemorySegment.ofArray(new byte[80]); // Create a memory segment of 80 bytes.

    // Set some initial data.
    keyValueLeafPage.setSlot(recordData, 0);

    // Check that there's not enough space for new data.
    assertFalse(keyValueLeafPage.hasEnoughSpace(offsets, memory, 100),
                "There should not be enough space for the new data.");
  }

  @Test
  void testHasEnoughSpaceWithLargeOffset() {
    // Simulate a situation with large offset values and small memory.
    byte[] recordData = new byte[20];
    int[] offsets = new int[]{0, 24, -1};

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
    MemorySegment memory = MemorySegment.ofArray(new byte[0]); // Empty memory segment.

    // Check that there's not enough space.
    assertFalse(keyValueLeafPage.hasEnoughSpace(offsets, memory, 1),
                "There should not be enough space with an empty memory segment.");
  }

  @Test
  void testHasEnoughSpaceAfterResizing() {
    // Simulate a situation where the memory is resized.
    byte[] recordData = new byte[50];
    int[] offsets = new int[5];
    Arrays.fill(offsets, -1); // Initially, no slots are occupied.
    MemorySegment memory = MemorySegment.ofArray(new byte[50]); // Small memory segment.

    // Set some initial data.
    keyValueLeafPage.setSlot(recordData, 0);

    // Resize memory segment to accommodate more data.
    memory = keyValueLeafPage.resizeMemorySegment(memory, 200, offsets);

    // Check that there's now enough space.
    assertTrue(keyValueLeafPage.hasEnoughSpace(offsets, memory, 100),
               "There should be enough space after resizing the memory segment.");
  }

  @Test
  void testSetSlotNewEntry() {
    byte[] recordData = new byte[50];
    int offset = 0;

    keyValueLeafPage.setSlot(recordData, offset);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(offset);

    assertArrayEquals(recordData, retrievedData, "The stored and retrieved data should be equal.");
  }

  @Test
  void testSetSlotUpdateEntry() {
    byte[] recordData = new byte[50];
    int offset = 0;

    keyValueLeafPage.setSlot(recordData, offset);

    // Update the slot with new data
    byte[] newRecordData = new byte[50];
    for (int i = 0; i < newRecordData.length; i++) {
      newRecordData[i] = (byte) (i + 1);
    }

    keyValueLeafPage.setSlot(newRecordData, offset);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(offset);

    assertArrayEquals(newRecordData, retrievedData, "The updated data should be retrieved.");
  }

  @Test
  void testSetSlotExceedingInitialCapacity() {
    byte[] recordData = new byte[50];
    int offset = 0;

    keyValueLeafPage.setSlot(recordData, offset);

    // Simulate setting a slot that requires more memory than initially allocated.
    byte[] largeRecordData = new byte[100];
    // Update the slot with new data
    for (int i = 0; i < largeRecordData.length; i++) {
      largeRecordData[i] = (byte) (i + 1);
    }

    keyValueLeafPage.setSlot(largeRecordData, offset);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(offset);

    assertArrayEquals(largeRecordData,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");
  }

  @Test
  void testSetSlotShiftRightWithFollowingSlotSet() {
    byte[] recordData = new byte[50];
    Arrays.fill(recordData, (byte) 1);

    keyValueLeafPage.setSlot(recordData, 0);

    Arrays.fill(recordData, (byte) 2);
    keyValueLeafPage.setSlot(recordData, 1);

    // Simulate setting a slot that requires more memory than initially allocated.
    byte[] largeRecordData = new byte[100];
    Arrays.fill(largeRecordData, (byte) 3);

    keyValueLeafPage.setSlot(largeRecordData, 0);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(0);

    assertArrayEquals(largeRecordData,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");

    retrievedData = keyValueLeafPage.getSlotAsByteArray(1);

    assertArrayEquals(recordData,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");
  }

  @Test
  void testSetSlotShiftLeftWithFollowingSlotSet() {
    byte[] recordData0 = new byte[50];
    Arrays.fill(recordData0, (byte) 1);

    keyValueLeafPage.setSlot(recordData0, 0);

    byte[] recordData1 = new byte[50];
    Arrays.fill(recordData1, (byte) 2);
    keyValueLeafPage.setSlot(recordData1, 1);

    // Simulate setting a slot that requires less memory than initially allocated.
    byte[] smallRecordData = new byte[20];
    Arrays.fill(smallRecordData, (byte) 3);

    keyValueLeafPage.setSlot(smallRecordData, 0);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(0);

    assertArrayEquals(smallRecordData,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");

    retrievedData = keyValueLeafPage.getSlotAsByteArray(1);

    assertArrayEquals(recordData1,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");
  }

  @Test
  void when_random_order_testSetSlotShiftLeftWithNoFollowingSlotSet() {
    byte[] recordData1 = new byte[50];
    Arrays.fill(recordData1, (byte) 2);
    keyValueLeafPage.setSlot(recordData1, 1);

    byte[] recordData0 = new byte[50];
    Arrays.fill(recordData0, (byte) 1);

    keyValueLeafPage.setSlot(recordData0, 0);

    // Simulate setting a slot that requires less memory than initially allocated.
    byte[] smallRecordData = new byte[20];
    Arrays.fill(smallRecordData, (byte) 3);

    keyValueLeafPage.setSlot(smallRecordData, 0);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(0);

    assertArrayEquals(smallRecordData,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");

    retrievedData = keyValueLeafPage.getSlotAsByteArray(1);

    assertArrayEquals(recordData1,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");
  }

  @Test
  void when_random_order_testSetSlotShiftLeftWithFollowingSlotSet() {
    byte[] recordData1 = new byte[50];
    Arrays.fill(recordData1, (byte) 1);
    keyValueLeafPage.setSlot(recordData1, 1);

    byte[] recordData0 = new byte[50];
    Arrays.fill(recordData0, (byte) 0);

    keyValueLeafPage.setSlot(recordData0, 0);

    byte[] recordData2 = new byte[50];
    Arrays.fill(recordData2, (byte) 2);

    keyValueLeafPage.setSlot(recordData2, 2);

    // Simulate setting a slot that requires less memory than initially allocated.
    byte[] smallRecordData = new byte[20];
    Arrays.fill(smallRecordData, (byte) 3);

    keyValueLeafPage.setSlot(smallRecordData, 0);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(0);

    assertArrayEquals(smallRecordData,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");

    retrievedData = keyValueLeafPage.getSlotAsByteArray(1);

    assertArrayEquals(recordData1,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");

    retrievedData = keyValueLeafPage.getSlotAsByteArray(2);

    assertArrayEquals(recordData2,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");
  }

  @Test
  void testSetSlotExceedingInitialCapacityWithFollowingSlotsSet() {
    byte[] recordData0 = new byte[50];
    Arrays.fill(recordData0, (byte) 1);
    keyValueLeafPage.setSlot(recordData0, 0);

    byte[] recordData1 = new byte[50];
    Arrays.fill(recordData1, (byte) 2);
    keyValueLeafPage.setSlot(recordData1, 1);

    byte[] recordData2 = new byte[50];
    Arrays.fill(recordData2, (byte) 3);
    keyValueLeafPage.setSlot(recordData2, 2);

    // Simulate setting a slot that requires more memory than initially allocated.
    byte[] largeRecordData = new byte[100];
    Arrays.fill(largeRecordData, (byte) 4);

    keyValueLeafPage.setSlot(largeRecordData, 0);
    byte[] retrievedData = keyValueLeafPage.getSlotAsByteArray(0);

    assertArrayEquals(largeRecordData,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");

    retrievedData = keyValueLeafPage.getSlotAsByteArray(1);

    assertArrayEquals(recordData1,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");

    retrievedData = keyValueLeafPage.getSlotAsByteArray(2);

    assertArrayEquals(recordData2,
                      retrievedData,
                      "The data should be stored even if it exceeds the initial capacity.");
  }

  @Test
  void testSetSlotMultipleSlots() {
    byte[] recordData1 = new byte[Constants.MAX_RECORD_SIZE];
    byte[] recordData2 = new byte[Constants.MAX_RECORD_SIZE];
    int offset1 = 0;
    int offset2 = 1;

    keyValueLeafPage.setSlot(recordData1, offset1);
    keyValueLeafPage.setSlot(recordData2, offset2);

    byte[] retrievedData1 = keyValueLeafPage.getSlotAsByteArray(offset1);
    byte[] retrievedData2 = keyValueLeafPage.getSlotAsByteArray(offset2);

    assertArrayEquals(recordData1, retrievedData1, "The first slot data should be retrieved correctly.");
    assertArrayEquals(recordData2, retrievedData2, "The second slot data should be retrieved correctly.");
  }

  @Test
  void testSetSlotInvalidOffset() {
    byte[] recordData = new byte[Constants.MAX_RECORD_SIZE];
    int invalidOffset = Constants.NDP_NODE_COUNT; // Out of bounds

    assertThrows(IndexOutOfBoundsException.class,
                 () -> keyValueLeafPage.setSlot(recordData, invalidOffset),
                 "Setting a slot at an invalid offset should throw an exception.");
  }
}
