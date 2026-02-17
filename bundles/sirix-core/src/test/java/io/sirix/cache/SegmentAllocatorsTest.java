package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SegmentAllocatorsTest {

  @Test
  void testRoundUpToPowerOfTwo_validSizes() {
    assertEquals(MemorySegmentAllocator.FOUR_KB, SegmentAllocators.roundUpToPowerOfTwo(3000));
    assertEquals(MemorySegmentAllocator.SIXTEEN_KB, SegmentAllocators.roundUpToPowerOfTwo(12000));
    assertEquals(MemorySegmentAllocator.SIXTYFOUR_KB, SegmentAllocators.roundUpToPowerOfTwo(50000));
    assertEquals(MemorySegmentAllocator.TWO_FIFTYSIX_KB,
        SegmentAllocators.roundUpToPowerOfTwo(MemorySegmentAllocator.TWO_FIFTYSIX_KB));
  }

  @Test
  void testRoundUpToPowerOfTwo_outOfRangeSizes() {
    assertThrows(IllegalArgumentException.class, () -> SegmentAllocators.roundUpToPowerOfTwo(0));
    assertThrows(IllegalArgumentException.class,
        () -> SegmentAllocators.roundUpToPowerOfTwo(MemorySegmentAllocator.TWO_FIFTYSIX_KB + 1));
  }

  @Test
  void testRoundUpToPowerOfTwo_edgeCases() {
    assertEquals(MemorySegmentAllocator.FOUR_KB, SegmentAllocators.roundUpToPowerOfTwo(MemorySegmentAllocator.FOUR_KB));
    assertEquals(MemorySegmentAllocator.TWO_FIFTYSIX_KB,
        SegmentAllocators.roundUpToPowerOfTwo(MemorySegmentAllocator.TWO_FIFTYSIX_KB - 1));
  }

  @Test
  void testGetIndexForSize_validSizes() {
    assertEquals(0, SegmentAllocators.getIndexForSize(MemorySegmentAllocator.FOUR_KB));
    assertEquals(1, SegmentAllocators.getIndexForSize(MemorySegmentAllocator.EIGHT_KB));
    assertEquals(6, SegmentAllocators.getIndexForSize(MemorySegmentAllocator.TWO_FIFTYSIX_KB));
  }

  @Test
  void testGetIndexForSize_nonPowerOfTwoSizes() {
    assertEquals(1, SegmentAllocators.getIndexForSize(5000)); // Rounded up to 8192
    assertEquals(2, SegmentAllocators.getIndexForSize(12000)); // Rounded up to 16384
  }

  @Test
  void testGetIndexForSize_edgeCases() {
    assertEquals(0, SegmentAllocators.getIndexForSize(MemorySegmentAllocator.FOUR_KB)); // Minimum size
    assertEquals(6, SegmentAllocators.getIndexForSize(MemorySegmentAllocator.TWO_FIFTYSIX_KB)); // Maximum size
  }
}
