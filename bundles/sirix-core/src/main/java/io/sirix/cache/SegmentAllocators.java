package io.sirix.cache;

import static com.google.common.base.Preconditions.checkArgument;
import static io.sirix.cache.MemorySegmentAllocator.SEGMENT_SIZES;

class SegmentAllocators {

  private static final int MIN_SIZE_LOG2 = 12; // log2(4096)

  /**
   * This class provides utility methods for segment allocation size calculations.
   */
  private SegmentAllocators() {
    // Prevent instantiation
    assert false : "This class should not be instantiated!";
  }

  static long roundUpToPowerOfTwo(long size) {
    long minimumSize = SEGMENT_SIZES[0];
    long maximumSize = SEGMENT_SIZES[SEGMENT_SIZES.length - 1];

    checkArgument(size > 0 && size <= maximumSize,
                  "Size must be between " + minimumSize + " and " + maximumSize + " bytes");

    long clamped = Math.max(size, minimumSize);
    return 1L << (Long.SIZE - Long.numberOfLeadingZeros(clamped - 1));
  }

  static int getIndexForSize(long size) {
    // Round up to the nearest power of two if not already a power of two
    boolean isPowerOfTwo = (size > 0) && (size & (size - 1)) == 0;

    long roundedSize =
        isPowerOfTwo ? Math.max(size, SEGMENT_SIZES[0]) : roundUpToPowerOfTwo(size);

    // Compute index: log2(roundedSize) - log2(SEGMENT_SIZES[0])
    int index = Long.SIZE - 1 - Long.numberOfLeadingZeros(roundedSize) - MIN_SIZE_LOG2;

    // Ensure the index is within the valid range of SEGMENT_SIZES
    return (index >= 0 && index < SEGMENT_SIZES.length) ? index : -1;
  }
}
