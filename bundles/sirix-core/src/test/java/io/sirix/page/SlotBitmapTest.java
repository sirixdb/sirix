package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the slot bitmap optimization in KeyValueLeafPage.
 * <p>
 * The slot bitmap provides O(k) iteration over populated slots instead of O(1024), where k is the
 * number of populated slots.
 */
class SlotBitmapTest {

  private Arena arena;
  private ResourceConfiguration resourceConfig;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    resourceConfig = new ResourceConfiguration.Builder("testResource").build();
  }

  @AfterEach
  void tearDown() {
    arena.close();
  }

  @Test
  void testEmptyPageHasNoBitsSet() {
    MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    // Empty page should have no bits set
    long[] bitmap = page.getSlotBitmap();
    for (long word : bitmap) {
      assertEquals(0L, word, "Empty page should have all bitmap bits cleared");
    }

    // populatedSlots() should return empty array
    assertEquals(0, page.populatedSlots().length);
    assertEquals(0, page.populatedSlotCount());

    page.close();
  }

  @Test
  void testSingleSlotSetsBit() {
    MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    // Set slot 42
    byte[] testData = new byte[] {1, 2, 3, 4};
    page.setSlot(testData, 42);

    // Check bitmap
    assertTrue(page.hasSlot(42), "Slot 42 should be marked as populated");
    assertFalse(page.hasSlot(41), "Slot 41 should not be marked");
    assertFalse(page.hasSlot(43), "Slot 43 should not be marked");

    // Check count
    assertEquals(1, page.populatedSlotCount());

    // Check iterator
    int[] slots = page.populatedSlots();
    assertEquals(1, slots.length);
    assertEquals(42, slots[0]);

    page.close();
  }

  @Test
  void testMultipleSlotsSetBits() {
    MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    // Set specific slots
    Set<Integer> expectedSlots = Set.of(0, 1, 63, 64, 100, 500, 1000, 1023);
    byte[] testData = new byte[] {1, 2, 3, 4};

    for (int slot : expectedSlots) {
      page.setSlot(testData, slot);
    }

    // Verify each slot
    for (int slot : expectedSlots) {
      assertTrue(page.hasSlot(slot), "Slot " + slot + " should be marked as populated");
    }

    // Verify count
    assertEquals(expectedSlots.size(), page.populatedSlotCount());

    // Verify iterator returns all expected slots in order
    int[] populatedArray = page.populatedSlots();
    Set<Integer> actualSlots = new HashSet<>();
    for (int slot : populatedArray) {
      actualSlots.add(slot);
    }
    assertEquals(expectedSlots, actualSlots);

    page.close();
  }

  @Test
  void testPopulatedSlotsIteratorOrder() {
    MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    // Set slots in random order
    int[] slotsToSet = {100, 50, 200, 10, 999, 500};
    byte[] testData = new byte[] {1, 2, 3, 4};

    for (int slot : slotsToSet) {
      page.setSlot(testData, slot);
    }

    // Iterator should return slots in ascending order
    int[] result = page.populatedSlots();
    assertEquals(slotsToSet.length, result.length);

    for (int i = 1; i < result.length; i++) {
      assertTrue(result[i] > result[i - 1],
          "Slots should be in ascending order, but got " + result[i - 1] + " before " + result[i]);
    }

    page.close();
  }

  @Test
  void testBitmapIterationMatchesFullIteration() {
    MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    // Set random slots
    Random random = new Random(42);
    Set<Integer> expectedSlots = new HashSet<>();
    byte[] testData = new byte[] {1, 2, 3, 4};

    for (int i = 0; i < 50; i++) {
      int slot = random.nextInt(Constants.NDP_NODE_COUNT);
      expectedSlots.add(slot);
      page.setSlot(testData, slot);
    }

    // Method 1: Full iteration (O(1024))
    Set<Integer> fullIterResult = new HashSet<>();
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      if (page.getSlot(i) != null) {
        fullIterResult.add(i);
      }
    }

    // Method 2: Bitmap iteration (O(k))
    Set<Integer> bitmapIterResult = new HashSet<>();
    for (int slot : page.populatedSlots()) {
      bitmapIterResult.add(slot);
    }

    // Results should be identical
    assertEquals(fullIterResult, bitmapIterResult, "Bitmap iteration should produce same result as full iteration");

    page.close();
  }

  @Test
  void testBitmapClearedOnReset() {
    MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    // Set some slots
    byte[] testData = new byte[] {1, 2, 3, 4};
    page.setSlot(testData, 10);
    page.setSlot(testData, 100);
    page.setSlot(testData, 500);

    assertEquals(3, page.populatedSlotCount());

    // Reset the page
    page.reset();

    // Bitmap should be cleared
    assertEquals(0, page.populatedSlotCount());
    assertFalse(page.hasSlot(10));
    assertFalse(page.hasSlot(100));
    assertFalse(page.hasSlot(500));

    page.close();
  }

  @Test
  void testBoundarySlots() {
    MemorySegment slotMemory = arena.allocate(SIXTYFOUR_KB);
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    byte[] testData = new byte[] {1, 2, 3, 4};

    // Test boundary slots (at word boundaries in bitmap)
    int[] boundarySlots = {0, 63, 64, 127, 128, 191, 192, 1023};

    for (int slot : boundarySlots) {
      page.setSlot(testData, slot);
    }

    int[] result = page.populatedSlots();
    assertEquals(boundarySlots.length, result.length);

    for (int i = 0; i < boundarySlots.length; i++) {
      assertEquals(boundarySlots[i], result[i], "Boundary slot " + boundarySlots[i] + " mismatch");
    }

    page.close();
  }

  @Test
  void testFullPage() {
    MemorySegment slotMemory = arena.allocate(512 * 1024); // Larger buffer for full page
    KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, resourceConfig, 1, slotMemory, null);

    byte[] testData = new byte[] {1};

    // Fill all 1024 slots
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      page.setSlot(testData, i);
    }

    assertEquals(Constants.NDP_NODE_COUNT, page.populatedSlotCount());

    // All bits should be set
    for (long word : page.getSlotBitmap()) {
      assertEquals(-1L, word, "All bitmap bits should be set for full page");
    }

    // Iterator should return all 1024 slots
    int[] result = page.populatedSlots();
    assertEquals(Constants.NDP_NODE_COUNT, result.length);

    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      assertEquals(i, result[i]);
    }

    page.close();
  }
}

